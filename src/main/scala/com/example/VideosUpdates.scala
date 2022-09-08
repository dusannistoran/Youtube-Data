package com.example

import com.example.Utils.extractCategory
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.slf4j.LoggerFactory


class VideosUpdates(topicNifi: String, broker: String) {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configSpark.getString("checkpointLocation")

  lazy val spark = SparkSession
    .builder()
    .config("spark.speculation", "false")
    .config("checkpointLocation", s"$checkpoint")
    //.config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .master(s"$sparkCores")
    .appName("consume videos metrics updates to console")
    .getOrCreate()

  LoggerFactory.getLogger(spark.getClass)
  spark.sparkContext.setLogLevel("WARN")

  import org.apache.spark.sql.functions._

  // getting Kafka stream
  val myStreamDf = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", broker)
    .option("subscribe", topicNifi)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .option("multiLine", "true")
    .load()

  val valueDf = myStreamDf.selectExpr("CAST(value AS STRING)")

  val mySchema = StructType(
    StructField("id", StringType) ::
      StructField("snippet", StructType(
        StructField("title", StringType) ::
          StructField("description", StringType) ::
          StructField("publishedAt", StringType) ::
          StructField("channelId", StringType) ::
          StructField("channelTitle", StringType) ::
          StructField("categoryId", StringType) ::
          StructField("defaultAudioLanguage", StringType) :: Nil
      )) ::
      StructField("contentDetails", StructType(
        StructField("duration", StringType) :: Nil
      )) ::
      StructField("statistics", StructType(
        StructField("viewCount", StringType) ::
          StructField("likeCount", StringType) ::
          StructField("commentCount", StringType) :: Nil
      )) :: Nil
  )

  val dfWithColumns = valueDf
    .withColumn("value", from_json(col("value"), mySchema))

  //println("dfWithColumns schema:")
  //dfWithColumns.printSchema()

  import org.apache.spark.sql.functions.udf
/*
  def extractValueFromMap: UserDefinedFunction = {
    udf((key: Int) => categories.get(key))
  }

 */

  val data = dfWithColumns
    .withColumn("current_time", current_timestamp())
    .withColumn("video_id", dfWithColumns.col("value.id"))
    .withColumn("video_title", dfWithColumns.col("value.snippet.title"))
    .withColumn("video_description", dfWithColumns.col("value.snippet.description"))
    .withColumn("published_at", dfWithColumns.col("value.snippet.publishedAt").cast(TimestampType))
    .withColumn("channel_id", dfWithColumns.col("value.snippet.channelId"))
    .withColumn("channel_title", dfWithColumns.col("value.snippet.channelTitle"))
    .withColumn("category_id", dfWithColumns.col("value.snippet.categoryId").cast(IntegerType))
    .withColumn("category", extractCategory(dfWithColumns.col("value.snippet.categoryId").cast(IntegerType)))
    .withColumn("language", dfWithColumns.col("value.snippet.defaultAudioLanguage"))
    .withColumn("duration", dfWithColumns.col("value.contentDetails.duration"))
    .withColumn("view_count", dfWithColumns.col("value.statistics.viewCount").cast(LongType))
    .withColumn("like_count", dfWithColumns.col("value.statistics.likeCount").cast(IntegerType))
    .withColumn("comment_count", dfWithColumns.col("value.statistics.commentCount").cast(IntegerType))
    .drop("value")

  def streamFromKafkaToConsole(): Unit = {

    println("data schema:")
    data.printSchema()

    data.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

  def streamFromKafkaToDruid(topicDruid: String): Unit = {

    data.select(to_json(struct("*")).alias("value"))
    data.selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        //Thread.sleep(500)
        batch.write
          .format("kafka")
          .option("kafka.bootstrap.servers", broker)
          .option("topic", topicDruid)
          .save()
      }
      .start()
      .awaitTermination()
  }

  def streamFromKafkaToPostgres(today: String): Unit = {

    println("data schema:")
    data.printSchema()

    val mode: SaveMode = SaveMode.Append
    data.writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        batch.write
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", "jdbc:postgresql://postgres_db:5432/youtube_updates")
          .option("dbtable", s"public.yt_videos_$today")
          .option("user", "postgres")
          .option("password", "FoolishPassword")
          .mode(mode)
          .save()
      }
      .start()
      .awaitTermination()
  }

}
