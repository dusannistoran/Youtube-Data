package com.example

import com.example.Utils.{extractDuration, extractMonth, getMonth, getYear}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory

class VideosYearMonth(topicNifi: String, broker: String) {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configSpark.getString("checkpointLocation")

  lazy val spark = SparkSession
    .builder()
    .config("spark.speculation", "false")
    .config("checkpointLocation", s"$checkpoint")
    .master(s"$sparkCores")
    .appName("consume videos published_at to Postgres")
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
    StructField("video_id", StringType, true) ::
      StructField("video_title", StringType, true) ::
      StructField("video_description", StringType, true) ::
      StructField("published_at", StringType, true) ::
      StructField("channel_id", StringType, true) ::
      StructField("channel_title", StringType, true) ::
      StructField("category_id", IntegerType, true) ::
      StructField("category", StringType, true) ::
      StructField("language", StringType, true) ::
      StructField("duration", StringType, true) ::
      StructField("view_count", DoubleType, true) ::
      StructField("like_count", IntegerType, true) ::
      StructField("comment_count", IntegerType, true) :: Nil
  )

  val dfWithColumns = valueDf
    .withColumn("value", from_json(col("value"), mySchema))

  val data = dfWithColumns
    .withColumn("video_id", dfWithColumns.col("value.video_id"))
    .withColumn("video_title", dfWithColumns.col("value.video_title"))
    .withColumn("video_description", dfWithColumns.col("value.video_description"))
    .withColumn("published_at", dfWithColumns.col("value.published_at"))
    .withColumn("channel_id", dfWithColumns.col("value.channel_id"))
    .withColumn("channel_title", dfWithColumns.col("value.channel_title"))
    .withColumn("category_id", dfWithColumns.col("value.category_id"))
    .withColumn("category", dfWithColumns.col("value.category"))
    .withColumn("language", dfWithColumns.col("value.language"))
    .withColumn("duration", dfWithColumns.col("value.duration"))
    .withColumn("view_count", dfWithColumns.col("value.view_count").cast(LongType))
    .withColumn("like_count", dfWithColumns.col("value.like_count"))
    .withColumn("comment_count", dfWithColumns.col("value.comment_count"))
    .drop("value")

  val enrichedDf = data
    .withColumn("year", getYear(col("published_at")))
    .withColumn("month_tmp", getMonth(col("published_at")))
    .withColumn("month", extractMonth(col("month_tmp")))
    .drop(col("month_tmp"))
    .withColumn("duration_seconds", extractDuration(col("duration")))
    .drop("duration")

  def streamFromKafkaToConsole(): Unit = {

    println("enrichedDf schema:")
    enrichedDf.printSchema()

    enrichedDf.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

  def streamFromKafkaToPostgres(): Unit = {

    println("enrichedDf schema:")
    enrichedDf.printSchema()

    val mode: SaveMode = SaveMode.Append
    enrichedDf.writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        batch.write
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", "jdbc:postgresql://postgres_db:5432/youtube_updates")
          .option("dbtable", s"public.youtube_videos_year_month")
          .option("user", "postgres")
          .option("password", "FoolishPassword")
          .mode(mode)
          .save()
      }
      .start()
      .awaitTermination()
  }

}
