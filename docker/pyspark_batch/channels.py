#!/usr/bin/python3

# ./spark/bin/spark-submit ./spark/pyspark_batch/channels.py
# ./spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.6 ./spark/pyspark_batch/channels.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, LongType

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("channels_statistics") \
    .getOrCreate()

quiet_logs(spark)

channels_statistics_raw_df = \
    spark.read.json("hdfs://namenode:9000/youtube1/channels/*/statistics/*", multiLine=True)

channels_statistics_df = channels_statistics_raw_df \
    .withColumn("items", explode("items")) \
    .select("*", col("items.*")) \
    .drop("etag") \
    .drop("kind", "pageInfo") \
    .withColumn("channel_id", col("id")) \
    .withColumn("channel_title", col("snippet.title")) \
    .withColumn("channel_description", col("snippet.description")) \
    .withColumn("published_at", to_timestamp(col("snippet.publishedAt"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
    .withColumn("country", col("snippet.country")) \
    .withColumn("subscriber_count", col("statistics.subscriberCount").cast(LongType())) \
    .withColumn("video_count", col("statistics.videoCount").cast(IntegerType())) \
    .withColumn("view_count", col("statistics.viewCount").cast(LongType())) \
    .drop("id", "statistics", "items", "brandingSettings", "contentDetails", "localizations", "snippet", "status")

print("channels_statistics_df schema:")
channels_statistics_df.printSchema()
channels_statistics_df_count = channels_statistics_df.count()
print("channels_statistics_df count:", channels_statistics_df_count)

# write to Postgres
channels_statistics_df.write \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://postgres_db:5432/youtube_batch") \
    .option("dbtable", "public.yt_channels") \
    .option("user", "postgres") \
    .option("password", "FoolishPassword") \
    .save()





