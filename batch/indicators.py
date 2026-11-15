"""Compute historical YouTube indicators from HDFS Parquet snapshots."""
import argparse

from pyspark.sql import SparkSession, Window, functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="hdfs://namenode:9000/data/youtube/bronze")
    parser.add_argument("--output", default="hdfs://namenode:9000/data/youtube/gold")
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("youtube-batch-indicators")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    events = spark.read.parquet(args.input)
    # Repeated publications are removed, while a later collection remains a new snapshot.
    snapshots = events.dropDuplicates(["video_id", "collected_at"])
    latest = (snapshots.withColumn("rank", F.row_number().over(
        Window.partitionBy("video_id").orderBy(F.col("collected_at").desc())))
              .filter("rank = 1").drop("rank"))
    latest.createOrReplaceTempView("videos")
    channels = spark.sql("""
        SELECT channel_id, max(channel_title) AS channel_title,
               count(*) AS video_count, sum(coalesce(view_count, 0)) AS total_views,
               round(avg(view_count), 2) AS avg_views,
               sum(coalesce(like_count, 0)) AS total_likes
        FROM videos GROUP BY channel_id
    """)
    by_day = (latest.groupBy(F.to_date("published_at").alias("published_date"))
              .agg(F.count("*").alias("video_count"),
                   F.sum(F.coalesce("view_count", F.lit(0))).alias("total_views")))
    quality = snapshots.agg(
        F.count("*").alias("snapshot_count"),
        F.countDistinct("video_id").alias("distinct_videos"),
        F.sum(F.col("view_count").isNull().cast("int")).alias("missing_views"),
        F.sum(F.col("like_count").isNull().cast("int")).alias("missing_likes"),
    )
    channels.write.mode("overwrite").parquet(f"{args.output}/channels")
    by_day.write.mode("overwrite").parquet(f"{args.output}/published_daily")
    quality.write.mode("overwrite").json(f"{args.output}/quality")
    quality.show(truncate=False)
    channels.orderBy(F.desc("total_views")).show(10, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
