"""Read a finite Kafka snapshot with the Spark cluster and archive it in HDFS."""
import argparse

from pyspark.sql import SparkSession, functions as F, types as T

SCHEMA = T.StructType([
    T.StructField("video_id", T.StringType()),
    T.StructField("collected_at", T.StringType()),
    T.StructField("published_at", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("channel_id", T.StringType()),
    T.StructField("channel_title", T.StringType()),
    T.StructField("category_id", T.StringType()),
    T.StructField("duration_iso8601", T.StringType()),
    T.StructField("view_count", T.LongType()),
    T.StructField("like_count", T.LongType()),
    T.StructField("comment_count", T.LongType()),
    T.StructField("query", T.StringType()),
])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka", default="kafka:29092")
    parser.add_argument("--topic", default="youtube.videos.raw")
    parser.add_argument("--output", default="hdfs://namenode:9000/data/youtube/bronze")
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("youtube-kafka-archive")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", args.kafka)
           .option("subscribe", args.topic)
           .option("startingOffsets", "earliest")
           .option("endingOffsets", "latest")
           .load())
    videos = (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("v"),
                         "topic", "partition", "offset", "timestamp")
              .select("v.*", "topic", "partition", "offset", F.col("timestamp").alias("kafka_timestamp"))
              .filter(F.col("video_id").isNotNull())
              .withColumn("collected_at", F.to_timestamp("collected_at"))
              .withColumn("published_at", F.to_timestamp("published_at"))
              .withColumn("collection_date", F.to_date("collected_at")))
    count = videos.count()
    if count:
        (videos.write.mode("append").partitionBy("collection_date")
         .parquet(args.output))
    print(f"Archived {count} valid Kafka events to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
