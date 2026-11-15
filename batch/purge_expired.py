"""Delete public YouTube API snapshots older than 30 days from HDFS."""
import argparse
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze", default="hdfs://namenode:9000/data/youtube/bronze")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("youtube-purge-expired").getOrCreate()
    jvm = spark._jvm
    root = jvm.org.apache.hadoop.fs.Path(args.bronze)
    fs = root.getFileSystem(spark._jsc.hadoopConfiguration())
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).date()
    deleted = 0
    if fs.exists(root):
        for status in fs.listStatus(root):
            name = status.getPath().getName()
            if not status.isDirectory() or not name.startswith("collection_date="):
                continue
            try:
                day = datetime.strptime(name.split("=", 1)[1], "%Y-%m-%d").date()
            except ValueError:
                continue
            if day < cutoff and fs.delete(status.getPath(), True):
                deleted += 1
    print(f"Deleted {deleted} expired partitions (before {cutoff}); rerun indicators afterward")
    spark.stop()


if __name__ == "__main__":
    main()
