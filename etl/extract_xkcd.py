#!/usr/bin/env python3
import json
import logging
import os
import re
import subprocess
import time
import sys

HDFS_RAW_PATH = "/user/hadoop/xkcd/raw"
WEB_URL = "https://xkcd.com"

FETCH_RETRY = 3 # number of retries for fetching JSON
RETRY_DELAY = 1.0  # delay between retries in seconds
MAX_CONSECUTIVE_FAILURES = 5 # maximum number of consecutive failures before giving up
REQUEST_TIMEOUT = 15  # timeout for HTTP requests in seconds

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ComicNotFoundError(Exception):
    """Raised when a comic is not found (HTTP 404)."""
    pass

def hdfs(*args):
    """Run a command on HDFS."""
    cmd = ["hdfs", "dfs"] + list(args)
    try:
        result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Error running HDFS command: {e.stderr}") from e

def get_existing_nums():
    nums = set()
    try:
        listing = hdfs("-ls", f"{HDFS_RAW_PATH}/*/*.json")
    except RuntimeError:
        logger.warning("No existing comics found in HDFS (first run?).")
        return nums
    for line in listing.splitlines():
        line = line.strip()
        if not line or line.startswith("Found"):
            continue
        filename = line.split()[-1]
        match = re.search(r"/(\d+)\.json$", filename)
        if match:
            nums.add(int(match.group(1)))
    return nums

def fetch_json(num, retries=FETCH_RETRY):
    url = f"{WEB_URL}/info.0.json" if num is None else f"{WEB_URL}/{num}/info.0.json"
    body = code = ""
    for attempt in range(1, retries + 1):
        result = subprocess.run(["curl", "-s", "--max-time", str(REQUEST_TIMEOUT), "-w", "\n%{http_code}", url], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        body, _, code = result.stdout.rpartition("\n")
        if result.returncode == 0 and code == "200":
            return json.loads(body)
        if code == "404":  # HTTP error
            raise ComicNotFoundError(f"Comic {num} not found.")
        if attempt < retries:
            delay = RETRY_DELAY * (2 ** (attempt - 1))  # exponential backoff
            logger.warning(f"fetch {num} failed (HTTP code: {code or 'none'}), retrying in {delay:.1f}s...")
            time.sleep(delay)
    raise RuntimeError(f"Failed to fetch JSON from {url} after {retries} attempts "
                       f"(last HTTP code: {code or 'none'}, last error: {result.stderr.strip()})")


def store_json(data):
    year = data.get("year")
    if year is None:
        raise ValueError("JSON data does not contain 'year' field.")
    path = f"{HDFS_RAW_PATH}/{year}/{data['num']}.json"
    hdfs("-mkdir", "-p", f"{HDFS_RAW_PATH}/{year}")
    subprocess.run(["hdfs", "dfs", "-put", "-f", "-", path], input=json.dumps(data), universal_newlines=True, check=True)

def main():
    if subprocess.run(["which", "hdfs"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode != 0:
        os.environ["PATH"] += ":/home/airflow/hadoop/bin"
    os.environ["HADOOP_USER_NAME"] = "hadoop"

    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None

    existing_nums = get_existing_nums()
    latest_num = fetch_json(None)["num"]

    todo = [num for num in range(1, latest_num + 1) if num not in existing_nums]
    if limit:
        todo = todo[:limit]
    if not todo:
        logger.info("No new comics to fetch. Newest comic number: %d, fetched: %d", latest_num, len(existing_nums))
        return

    logger.info("Fetching %d new comics (latest comic number: %d)", len(todo), latest_num)
    ok = fail = skipped = 0
    consecutive_failures = 0

    for num in todo:
        try:
            data = fetch_json(num)
            store_json(data)
        except ComicNotFoundError:
            skipped += 1
            consecutive_failures = 0
            logger.warning("Comic %d not found (404). Skipping.", num)
        except (RuntimeError, subprocess.CalledProcessError, ValueError) as e:
            fail += 1
            consecutive_failures += 1
            logger.error("Failed to fetch or store comic %d: %s", num, e)
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                logger.error("Maximum consecutive failures reached (%d). Stopping.", MAX_CONSECUTIVE_FAILURES)
                sys.exit(1)
            time.sleep(RETRY_DELAY)
            continue
        ok += 1
        consecutive_failures = 0
        time.sleep(RETRY_DELAY)
    logger.info("Done. Fetched: %d, Failed: %d, Skipped: %d", ok, fail, skipped)
    if ok == 0 and fail > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()