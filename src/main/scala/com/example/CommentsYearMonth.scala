package com.example

import com.example.Utils.{extractMonth, getMonth, getYear}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory

class CommentsYearMonth(topicNifi: String, broker: String) {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configSpark.getString("checkpointLocation")

  lazy val spark = SparkSession
    .builder()
    .config("spark.speculation", "false")
    .config("checkpointLocation", s"$checkpoint")
    .master(s"$sparkCores")
    .appName("consume comments published_at to Postgres")
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
    StructField("comment_id", StringType, true) ::
    StructField("video_id", StringType, true) ::
      StructField("comment_text", StringType, true) ::
      StructField("comment_author_channel_name", StringType, true) ::
      StructField("comment_author_channel_id", StringType, true) ::
      StructField("comment_author_channel_url", StringType, true) ::
      StructField("published_at", StringType, true) ::
      StructField("comment_like_count", IntegerType, true) ::
      StructField("total_reply_count", IntegerType, true) ::
      StructField("text_length", IntegerType) :: Nil
  )

  val dfWithColumns = valueDf
    .withColumn("value", from_json(col("value"), mySchema))

  val data = dfWithColumns
    .withColumn("comment_id", dfWithColumns.col("value.comment_id"))
    .withColumn("video_id", dfWithColumns.col("value.video_id"))
    .withColumn("comment_text", dfWithColumns.col("value.comment_text"))
    .withColumn("comment_author_channel_title", dfWithColumns.col("value.comment_author_channel_name"))
    .withColumn("comment_author_channel_id", dfWithColumns.col("value.comment_author_channel_id"))
    .withColumn("comment_author_channel_url", dfWithColumns.col("value.comment_author_channel_url"))
    .withColumn("published_at", dfWithColumns.col("value.published_at"))
    .withColumn("comment_like_count", dfWithColumns.col("value.comment_like_count"))
    .withColumn("total_reply_count", dfWithColumns.col("value.total_reply_count"))
    .withColumn("text_length", dfWithColumns.col("value.text_length"))
    .drop("value")

  val enrichedDf = data
    .withColumn("year", getYear(col("published_at")))
    .withColumn("month_tmp", getMonth(col("published_at")))
    .withColumn("month", extractMonth(col("month_tmp")))
    .drop(col("month_tmp"))

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
          .option("dbtable", s"public.youtube_comments_year_month")
          .option("user", "postgres")
          .option("password", "FoolishPassword")
          .mode(mode)
          .save()
      }
      .start()
      .awaitTermination()
  }

}
