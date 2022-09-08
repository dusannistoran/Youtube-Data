#!/usr/bin/python3

# ./spark/bin/spark-submit ./spark/pyspark_batch/playlists.py
# ./spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.6 ./spark/pyspark_batch/playlists.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, LongType, StringType

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("playlists") \
    .getOrCreate()

quiet_logs(spark)

playlists_raw_df \
    = spark.read.json("hdfs://namenode:9000/youtube1/channels/*/playlists/*", multiLine=True)
'''
print("playlists_raw_df schema:")
playlists_raw_df.printSchema()
playlists_raw_df_count = playlists_raw_df.count()
print("playlists_raw_df count:", playlists_raw_df.count())
playlists_raw_df.show(15, vertical=True)
'''

playlists_df = playlists_raw_df \
    .drop("etag", "kind", "nextPageToken", "pageInfo") \
    .withColumn("items", explode("items")) \
    .select("*", col("items.*")) \
    .withColumn("playlist_id", col("items.id")) \
    .withColumn("playlist_title", col("items.snippet.title")) \
    .withColumn("playlist_description", col("items.snippet.description")) \
    .withColumn("published_at", to_timestamp(col("items.snippet.publishedAt"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
    .withColumn("channel_id", col("items.snippet.channelId")) \
    .withColumn("channel_title", col("items.snippet.channelTitle")) \
    .withColumn("videos_count", col("items.contentDetails.itemCount")) \
    .drop("items", "contentDetails", "etag", "id", "kind", "snippet", "localizations", "player", "status")

print("playlists_df schema:")
playlists_df.printSchema()
playlists_df_count = playlists_df.count()
print("playlists_df count:", playlists_df.count())
playlists_df.show(15, vertical=True)

# write to Postgres
playlists_df.write \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://postgres_db:5432/youtube_batch") \
    .option("dbtable", "public.yt_playlists") \
    .option("user", "postgres") \
    .option("password", "FoolishPassword") \
    .save()


