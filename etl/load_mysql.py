#!/usr/bin/env python3

import os
import pymysql
from pyspark.sql import SparkSession


HDFS_FINAL_PATH = "hdfs://hadoop:9000/user/hadoop/xkcd/final"

MYSQL = {
    "host": os.environ.get("MYSQL_HOST", "mysql"),
    "user": os.environ.get("MYSQL_USER", "root"),
    "password": os.environ.get("MYSQL_PASSWORD", "xkcd"),
    "database": os.environ.get("MYSQL_DATABASE", "xkcd"),
    "charset": "utf8mb4",
}

UPSERT = """INSERT INTO comics (num, title, alt, img, link, pub_date, search_text)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                title = VALUES(title),
                alt = VALUES(alt),
                img = VALUES(img),
                link = VALUES(link),
                pub_date = VALUES(pub_date),
                search_text = VALUES(search_text)"""

def write_partition(rows):
    conn = pymysql.connect(**MYSQL)
    try:
        with conn.cursor() as cursor:
            cursor.executemany(UPSERT, [(r.num, r.title, r.alt, r.img, r.link, r.pub_date, r.search_text) for r in rows])
        conn.commit()
    finally:
        conn.close()

def main():
    spark = SparkSession.builder.appName("XKCD Load MySQL").getOrCreate()
    df = spark.read.csv(HDFS_FINAL_PATH, header=True)
    df.foreachPartition(write_partition)
    spark.stop()

if __name__ == "__main__":
    main()