import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


HDFS_RAW_PATH = "hdfs:///user/hadoop/xkcd/raw"
HDFS_FINAL_PATH = "hdfs:///user/hadoop/xkcd/final"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder.appName("XKCD Transform Final").getOrCreate()

    # 1. Read the raw JSON files from HDFS
    raw_df = (spark.read.option("multiline", "true").json(f"{HDFS_RAW_PATH}/*/*.json"))
    raw_count = raw_df.count()
    if raw_count == 0:
        raise RuntimeError(f"No raw JSON files found in {HDFS_RAW_PATH}. Run extract_xkcd.py first.")

    # 2. Clean and transform the data
    cleaned_df = (raw_df
                  .filter(F.col("num").isNotNull())
                    .withColumn("num", F.col("num").cast("int"))
                    .withColumn("title", F.coalesce("safe_title", "title"))
                    .withColumn("link", F.concat(F.lit("https://xkcd.com/"), F.col("num"), F.lit("/")))
                    .withColumn("pub_date", F.to_date(F.concat_ws("-", "year", "month", "day"), "yyyy-M-d"))
                    .withColumn("search_text", F.concat_ws(" ", "title", "alt"))
                    .select("num", "pub_date", "title", "link", "img", "alt", "search_text")
                    .dropDuplicates(["num"]))
    final_count = cleaned_df.count()

    (cleaned_df.write
        .mode("overwrite")
        .option("header", "true")
        .option("quoteAll", "true")
        .option("escape", '"')
        .csv(HDFS_FINAL_PATH))

    logger.info(f"Transformed {raw_count} raw records into {final_count} final records and saved to {HDFS_FINAL_PATH} ({raw_count - final_count} duplicates removed).")
    spark.stop()

if __name__ == "__main__":
    main()