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
REQUEST_PAUSE = 0.5  # pause between requests to avoid overloading the server
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
    """Get a set of comic numbers that already exist in HDFS."""
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
    """Fetch JSON data for a specific comic number from the XKCD website."""
    url = f"{WEB_URL}/info.0.json" if num is None else f"{WEB_URL}/{num}/info.0.json"
    body = code = ""
    for attempt in range(1, retries + 1):
        result = subprocess.run(["curl", "-s", "--max-time", str(REQUEST_TIMEOUT), "-w", "\n%{http_code}", url], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        body, _, code = result.stdout.rpartition("\n")
        if result.returncode == 0 and code == "200":
            try:
                data = json.loads(body)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON: comic={url}, try={attempt}, error={e}")
                raise
            logger.info(f"Successfully fetched JSON for: comic {num or 'latest'}, api_num={data.get('num')}, title={data.get('safe_title') or data.get('title')}, year={data.get('year')}, attempt={attempt}")
            return data
        if code == "404":  # HTTP error
            logger.warning(f"Comic {num} not found (HTTP 404).")
            raise ComicNotFoundError(f"Comic {num} not found.")
        logger.warning(f"Failed to fetch JSON: comic={num or 'latest'}, HTTP code=={code or 'none'}, try={attempt}/{retries}")
        if attempt < retries:
            delay = RETRY_DELAY * (2 ** (attempt - 1))  # exponential backoff
            logger.warning(f"fetch {num} failed (HTTP code: {code or 'none'}), retrying in {delay:.1f}s...")
            time.sleep(delay)
    raise RuntimeError(f"Failed to fetch JSON from {url} after {retries} attempts "
                       f"(last HTTP code: {code or 'none'}, last error: {result.stderr.strip()})")

_existing_year_dirs = set()

def store_json(data):
    """Store JSON data in HDFS under the appropriate year directory."""
    year = data.get("year")
    if year is None:
        raise ValueError("JSON data does not contain 'year' field.")
    if year not in _existing_year_dirs:
        logger.info(f"Creating HDFS directory for year {year}: {HDFS_RAW_PATH}/{year}")
        hdfs("-mkdir", "-p", f"{HDFS_RAW_PATH}/{year}")
        _existing_year_dirs.add(year)
    path = f"{HDFS_RAW_PATH}/{year}/{data['num']}.json"
    try:
        subprocess.run(["hdfs", "dfs", "-put", "-f", "-", path], input=json.dumps(data), universal_newlines=True, check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to store JSON for comic {data.get('num')} in HDFS: {e}")
        raise
    logger.info(f"Stored JSON for comic {data.get('num')} in HDFS at {path}")

def main():
    if subprocess.run(["which", "hdfs"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode != 0:
        base = os.environ.get("PATH") or "/usr/local/bin:/usr/bin:/bin"
        os.environ["PATH"] = base + ":/home/airflow/hadoop/bin"
    os.environ["HADOOP_USER_NAME"] = "hadoop"

    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None
    if limit == 0:
        limit = None

    existing_nums = get_existing_nums()
    latest_data = fetch_json(None)
    latest_num = fetch_json(None)["num"]

    logger.info(f"Latest available comic: Number={latest_num}, Title={latest_data.get('safe_title') or latest_data.get('title')}, Year={latest_data.get('year')}, Month={latest_data.get('month')}, Day={latest_data.get('day')}")

    todo = [num for num in range(1, latest_num + 1) if num not in existing_nums] # start from 1 to latest_num inclusive, maybe give option to start from latest_num
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
            time.sleep(REQUEST_PAUSE)  # pause to avoid overloading the server
            continue
        ok += 1
        consecutive_failures = 0
        time.sleep(REQUEST_PAUSE)  # pause to avoid overloading the server
    logger.info("Done. Fetched: %d, Failed: %d, Skipped: %d", ok, fail, skipped)
    if ok == 0 and fail > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()
