#!/usr/bin/python3

# ./spark/bin/spark-submit ./spark/pyspark_batch/videos.py
# ./spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.6 ./spark/pyspark_batch/videos.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, LongType, StringType

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

categories = {
               1: 'Film & Animation',
               2: 'Autos & Vehicles',
              10: 'Music',
              15: 'Pets & Animals',
              17: 'Sports',
              18: 'Short Movies',
              19: 'Travel & Events',
              20: 'Gaming',
              21: 'Videoblogging',
              22: 'People & Blogs',
              23: 'Comedy',
              24: 'Entertainment',
              25: 'News & Politics',
              26: 'Howto & Style',
              27: 'Education',
              28: 'Science & Technology',
              29: 'Nonprofits & Activism',
              30: 'Movies',
              31: 'Anime/Animation',
              32: 'Action/Adventure',
              33: 'Classics',
              34: 'Comedy',
              35: 'Documentary',
              36: 'Drama',
              37: 'Family',
              38: 'Foreign',
              39: 'Horror',
              40: 'Sci-Fi/Fantasy',
              41: 'Thriller',
              42: 'Shorts',
              43: 'Shows',
              44: 'Trailers'
}

def post_categories(category_id):
    return categories[category_id]

class MyUDF():
    @staticmethod
    def get_category(categories):
        def post_categories(category_id):
            return categories[category_id]
        return udf(post_categories, StringType())

spark = SparkSession \
    .builder \
    .appName("videos") \
    .getOrCreate()

quiet_logs(spark)

channels_videos_statistics_raw_df \
    = spark.read.json("hdfs://namenode:9000/youtube1/channels/*/videos/*/statistics", multiLine=True)

channels_videos_statistics_df = channels_videos_statistics_raw_df \
    .withColumn("items", explode("items")) \
    .select("*", col("items.*")) \
    .drop("etag", "kind", "pageInfo") \
    .withColumn("video_id", col("items.id")) \
    .withColumn("published_at", to_timestamp(col("items.snippet.publishedAt"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
    .withColumn("channel_id", col("items.snippet.channelId")) \
    .withColumn("channel_title", col("items.snippet.channelTitle")) \
    .withColumn("video_title", col("items.snippet.title")) \
    .withColumn("video_description", col("items.snippet.description")) \
    .withColumn("view_count", col("items.statistics.viewCount").cast(LongType())) \
    .withColumn("like_count", col("items.statistics.likeCount").cast(IntegerType())) \
    .withColumn("comment_count", col("items.statistics.commentCount").cast(IntegerType())) \
    .withColumn("category_id", col("items.snippet.categoryId").cast(IntegerType())) \
    .withColumn("category", MyUDF.get_category(categories)(col("items.snippet.categoryId").cast(IntegerType()))) \
    .withColumn("language", col("items.snippet.defaultAudioLanguage")) \
    .withColumn("made_for_kids", col("items.status.madeForKids")) \
    .drop("items","id", "snippet", "statistics", "contentDetails", "liveStreamingDetails", "localizations") \
    .drop("player", "recordingDetails", "status", "topicDetails")

print("channels_videos_statistics_df schema:")
channels_videos_statistics_df.printSchema()
channels_videos_statistics_df_count = channels_videos_statistics_df.count()
print("channels_videos_statistics_df count:", channels_videos_statistics_df.count())
channels_videos_statistics_df.show(15, vertical=True)

# write to Postgres
channels_videos_statistics_df.write \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://postgres_db:5432/youtube_batch") \
    .option("dbtable", "public.yt_videos") \
    .option("user", "postgres") \
    .option("password", "FoolishPassword") \
    .save()

