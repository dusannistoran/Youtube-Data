package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.slf4j.LoggerFactory

class ChannelsConsumer(topic: String, broker: String) {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val sparkCores: String = configSpark.getString("master")
  //val HDFS_HOME: String = configSpark.getString("hdfsHome")
  val checkpoint: String = configSpark.getString("checkpointLocation")

  lazy val spark = SparkSession
    .builder()
    .config("spark.speculation", "false")
    .config("checkpointLocation", s"$checkpoint")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .master(s"$sparkCores")
    .appName("consume channels stream to console")
    .getOrCreate()

  LoggerFactory.getLogger(spark.getClass)
  spark.sparkContext.setLogLevel("WARN")

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", broker)
    .option("subscribe", topic)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()

  val channelsSchema = StructType(
    StructField("channel_id", StringType) ::
      StructField("subscriber_count", LongType) ::
      StructField("video_count", IntegerType) ::
      StructField("view_count", LongType) :: Nil
  )

  val valueDf = df.selectExpr("CAST(value AS STRING)")
  import org.apache.spark.sql.functions._
  val dfWithColumns = valueDf
    .withColumn("value", from_json(col("value"), channelsSchema))

  print("dfWithColumns schema:")
  dfWithColumns.printSchema()

  val data = dfWithColumns
    .withColumn("channel_id", dfWithColumns.col("value.channel_id"))
    .withColumn("subscriber_count", dfWithColumns.col("value.subscriber_count"))
    .withColumn("video_count", dfWithColumns.col("value.video_count"))
    .withColumn("view_count", dfWithColumns.col("value.view_count"))
    .drop("value")

  def streamFromKafkaToConsole(): Unit = {

    println("data schema:")
    data.printSchema()

    data.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      //.trigger(Trigger.Once())
      //.trigger(Trigger.ProcessingTime("49 seconds"))
      .start()
      .awaitTermination()
  }

  def streamFromKafkaToDruid(topicDruid: String): Unit = {

    println("data schema:")
    data.printSchema()

    data.selectExpr("CAST(channel_id AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers", broker)
      .option("topic", topicDruid)
      .option("checkpointLocation", s"$checkpoint")
      .start()
      .awaitTermination()
  }

}
