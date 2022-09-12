package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.slf4j.LoggerFactory

class ChannelsUpdates(topicNifi: String, broker: String) {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configSpark.getString("checkpointLocation")

  lazy val spark = SparkSession
    .builder()
    .config("spark.speculation", "false")
    .config("checkpointLocation", s"$checkpoint")
    .master(s"$sparkCores")
    .appName("consume channels metrics updates to console and to druid")
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
          StructField("country", StringType) :: Nil)) ::
        StructField("statistics", StructType(
          StructField("subscriberCount", StringType) ::
            StructField("videoCount", StringType) ::
            StructField("viewCount", StringType) :: Nil
        )
      ) :: Nil
  )

  val dfWithColumns = valueDf
    .withColumn("value", from_json(col("value"), mySchema))

  val data = dfWithColumns
    .withColumn("current_time", current_timestamp())
    .withColumn("channel_id", dfWithColumns.col("value.id"))
    .withColumn("channel_title", dfWithColumns.col("value.snippet.title"))
    .withColumn("channel_description", dfWithColumns.col("value.snippet.description"))
    .withColumn("published_at", dfWithColumns.col("value.snippet.publishedAt").cast(TimestampType))
    .withColumn("country", dfWithColumns.col("value.snippet.country"))
    .withColumn("subscriber_count", dfWithColumns.col("value.statistics.subscriberCount").cast(LongType))
    .withColumn("video_count", dfWithColumns.col("value.statistics.videoCount").cast(IntegerType))
    .withColumn("view_count", dfWithColumns.col("value.statistics.viewCount").cast(LongType))
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
        Thread.sleep(1000)
        batch.write
          .format("kafka")
          .option("kafka.bootstrap.servers", broker)
          .option("topic", topicDruid)
          .save()
      }
      .start()
      .awaitTermination()
  }


  /*
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
          .option("dbtable", s"public.yt_channels_$today")
          .option("user", "postgres")
          .option("password", "FoolishPassword")
          .mode(mode)
          .save()
      }
      .start()
      .awaitTermination()
  }
   */

}
