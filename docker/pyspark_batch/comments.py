#!/usr/bin/python3

# ./spark/bin/spark-submit ./spark/pyspark_batch/comments.py
# ./spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.6 ./spark/pyspark_batch/comments.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, LongType

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("channels_videos_comments") \
    .getOrCreate()

quiet_logs(spark)

channels_videos_comments_raw_df \
    = spark.read.json("hdfs://namenode:9000/youtube1/channels/*/videos/*/content", multiLine=True)

channels_videos_comments_df = channels_videos_comments_raw_df \
    .drop("etag", "kind", "nextPageToken", "pageInfo") \
    .withColumn("items", explode("items")) \
    .select("*", col("items.*")) \
    .withColumn("comment_id", col("items.id")) \
    .withColumn("video_id", col("items.snippet.videoId")) \
    .withColumn("comment_text", col("items.snippet.topLevelComment.snippet.textDisplay")) \
    .withColumn("comment_author_channel_name", col("items.snippet.topLevelComment.snippet.authorDisplayName")) \
    .withColumn("comment_author_channel_id", col("items.snippet.topLevelComment.snippet.authorChannelId.value")) \
    .withColumn("comment_author_channel_url", col("items.snippet.topLevelComment.snippet.authorChannelUrl")) \
    .withColumn("comment_like_count", col("items.snippet.topLevelComment.snippet.likeCount").cast(IntegerType())) \
    .withColumn("published_at", to_timestamp(col("items.snippet.topLevelComment.snippet.publishedAt"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
    .withColumn("total_reply_count", col("items.snippet.totalReplyCount").cast(IntegerType())) \
    .drop("items", "etag", "id", "kind", "snippet", "replies")

print("channels_videos_comments_df schema:")
channels_videos_comments_df.printSchema()
channels_videos_comments_df_count = channels_videos_comments_df.count()
print("channels_videos_comments_df count:", channels_videos_comments_df_count)
channels_videos_comments_df.show(15, vertical=True)

# write to Postgres
channels_videos_comments_df.write \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://postgres_db:5432/youtube_batch") \
    .option("dbtable", "public.yt_comments") \
    .option("user", "postgres") \
    .option("password", "FoolishPassword") \
    .save()
