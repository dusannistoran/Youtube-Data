package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory
import com.example.Utils.{extractMonth, getYear,getMonth}

class ChannelsYearMonth(topicNifi: String, broker: String) {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configSpark.getString("checkpointLocation")

  lazy val spark = SparkSession
    .builder()
    .config("spark.speculation", "false")
    .config("checkpointLocation", s"$checkpoint")
    .master(s"$sparkCores")
    .appName("consume channels published_at to Postgres")
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
    StructField("channel_id", StringType, true) ::
      StructField("channel_title", StringType, true) ::
      StructField("channel_description", StringType, true) ::
      StructField("published_at", StringType, true) ::
      StructField("country", StringType, true) ::
      StructField("subscriber_count", LongType, true) ::
      StructField("video_count", IntegerType, true) ::
      StructField("view_count", LongType, true) :: Nil
      )

  val dfWithColumns = valueDf
    .withColumn("value", from_json(col("value"), mySchema))

  val data = dfWithColumns
    .withColumn("channel_id", dfWithColumns.col("value.channel_id"))
    .withColumn("channel_title", dfWithColumns.col("value.channel_title"))
    .withColumn("channel_description", dfWithColumns.col("value.channel_description"))
    .withColumn("published_at", dfWithColumns.col("value.published_at"))
    .withColumn("country", dfWithColumns.col("value.country"))
    .withColumn("subscriber_count", dfWithColumns.col("value.subscriber_count"))
    .withColumn("video_count", dfWithColumns.col("value.video_count"))
    .withColumn("view_count", dfWithColumns.col("value.view_count"))
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
          .option("dbtable", s"public.youtube_channels_year_month")
          .option("user", "postgres")
          .option("password", "FoolishPassword")
          .mode(mode)
          .save()
      }
      .start()
      .awaitTermination()
  }

}
