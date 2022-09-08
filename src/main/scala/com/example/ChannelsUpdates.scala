package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.slf4j.LoggerFactory

import java.text.SimpleDateFormat
import java.time.LocalDate

class ChannelsUpdates(topicNifi: String, broker: String) {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configSpark.getString("checkpointLocation")

  lazy val spark = SparkSession
    .builder()
    .config("spark.speculation", "false")
    .config("checkpointLocation", s"$checkpoint")
    //.config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .master(s"$sparkCores")
    .appName("consume channels metrics updates to console")
    .getOrCreate()

  LoggerFactory.getLogger(spark.getClass)
  spark.sparkContext.setLogLevel("WARN")

  //val dfBatch = spark
   // .read
  //  .format("kafka")
   // .option("kafka.bootstrap.servers", broker)
   // .option("subscribe", topicNifi)
   // .option("inferSchema", "true")
   // .option("multiLine", "true")
   // .option("startingOffsets", "earliest")
   // .option("failOnDataLoss", "false")
   // .load()

  // first, we get value field from Kafka batch
  //val valueDfBatch = dfBatch.selectExpr("CAST(value AS STRING)")
  //println("valueDfBatch schema:")
  //valueDfBatch.printSchema()

  import org.apache.spark.sql.functions._

  // now, we write dataframe locally as text, but it's json
  //valueDfBatch.write.mode("overwrite").format("json").save("/home/dusan/Desktop/youtubeDataProject/src/main/scala/com/example/proba1")
  // here, we get the schema
  //val rawSchema = spark.read.json("/home/dusan/Desktop/youtubeDataProject/src/main/scala/com/example/proba1/*").schema

  //val rawWithColumns = valueDfBatch
 //   .withColumn("value", from_json(col("value"), rawSchema))

  //println("rawWithColumns schema: ")
  //rawWithColumns.printSchema()

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

  //val myBatchDf = spark
  //  .read
  //  .format("kafka")
  //  .option("kafka.bootstrap.servers", broker)
  //  .option("subscribe", topicNifi)
  //  .option("startingOffsets", "earliest")
  //  .option("multiLine", "true")
  //  .load()

  val valueDf = myStreamDf.selectExpr("CAST(value AS STRING)")
  //val valueDfBatch = myBatchDf.selectExpr("CAST(value AS STRING)")

  //val mySchema = StructType(
  //  StructField("kind", StringType) ::
  //    StructField("etag", StringType) ::
  //    StructField("items", ArrayType(
  //      StructType(StructField("id", StringType) ::
  //         Nil)))
  //    :: Nil
  //)

  //val schema = StructType(
  //  StructField("items", ArrayType(StructType(StructField("items", StringType))) :: Nil
  //)
  //  .add("items", ArrayType(StringType))
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

  //println("dfWithColumns schema:")
  //dfWithColumns.printSchema()

  val data = dfWithColumns
    //.withColumn("items", explode(dfWithColumns.col("value.items")))
    //.withColumn("items", explode(col("value.items")))
    //.select(col("*"), col("items.*"))
    //.withColumn("channel_id", dfWithColumns.col("value.items.id"))
    //.withColumn("channel_title", dfWithColumns.col("items.snippet.title"))
    //.drop("value")
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
    //.withColumn("channel_title", dfWithColumns.col("value.snippet.title"))

  //val dataBatch = dfWithColumnsBatch
  //  .withColumn("current_time", current_timestamp())
  //  .withColumn("channel_id", dfWithColumnsBatch.col("value.id"))
  //  .withColumn("channel_title", dfWithColumnsBatch.col("value.snippet.title"))
  //  .withColumn("channel_description", dfWithColumnsBatch.col("value.snippet.description"))
  //  .withColumn("published_at", dfWithColumnsBatch.col("value.snippet.publishedAt").cast(TimestampType))
  //  .withColumn("country", dfWithColumnsBatch.col("value.snippet.country"))
  //  .withColumn("subscriber_count", dfWithColumnsBatch.col("value.statistics.subscriberCount").cast(LongType))
  //  .withColumn("video_count", dfWithColumnsBatch.col("value.statistics.videoCount").cast(IntegerType))
  //  .withColumn("view_count", dfWithColumnsBatch.col("value.statistics.viewCount").cast(LongType))
  //  .drop("value")


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

  /*
  def streamFromKafkaToDruid(topicDruid: String): Unit = {

    println("data schema:")
    data.printSchema()

    data.selectExpr("CAST(current_time AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers", broker)
      .option("topic", topicDruid)
      .option("checkpointLocation", s"$checkpoint")
      .start()
      .awaitTermination()
  }
   */

  def streamFromKafkaToDruid(topicDruid: String): Unit = {
    //data.selectExpr("CAST(current_time AS STRING) AS key", "to_json(struct(*)) AS value")

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
  def batchFromKafkaToDruid(topicDruid: String): Unit = {

    println("dataBatch schema:")
    dataBatch.printSchema()

    dataBatch
      .select(to_json(struct("*")).as("value"))
      .writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        batch.write
          .format("kafka")
          .option("kafka.bootstrap.servers", broker)
          .option("topic", topicDruid)
          .save()
      }
      .start()
      .awaitTermination()
  }
   */

  def writeDataframeToHDFS(path: String): Unit = {

    println("data schema:")
    data.printSchema()

    //val today: String = LocalDate.now().toString
    //println("Today is " + today)

    data.writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        batch.write
          .format("json")
          //.json(path)
          .mode("append")
          //.mode(SaveMode.ErrorIfExists)
          //.saveAsTable(s"${today}_channels_updates")
          .save(path)
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
          .option("dbtable", s"public.yt_channels_$today")
          .option("user", "postgres")
          .option("password", "FoolishPassword")
          .mode(mode)
          .save()
      }
      .start()
      .awaitTermination()
  }

}
