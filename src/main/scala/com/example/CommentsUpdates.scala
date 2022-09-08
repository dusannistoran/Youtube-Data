package com.example

import com.example.Utils.extractCategory
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.slf4j.LoggerFactory

class CommentsUpdates(topicNifi: String, broker: String) {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configSpark.getString("checkpointLocation")

  lazy val spark = SparkSession
    .builder()
    .config("spark.speculation", "false")
    .config("checkpointLocation", s"$checkpoint")
    //.config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .master(s"$sparkCores")
    .appName("consume comments metrics updates to console")
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
        StructField("videoId", StringType) ::
          StructField("topLevelComment", StructType(
            StructField("snippet", StructType(
              StructField("textDisplay", StringType) ::
                StructField("authorDisplayName", StringType) ::
                StructField("authorChannelId", StructType(
                  StructField("value", StringType) :: Nil
                )) ::
                StructField("authorChannelUrl", StringType) ::
                StructField("publishedAt", StringType) ::
                StructField("likeCount", IntegerType) :: Nil
            )
          ) :: Nil
          )) ::
          StructField("totalReplyCount", IntegerType) :: Nil
      )) :: Nil
  )

  val dfWithColumns = valueDf
    .withColumn("value", from_json(col("value"), mySchema))

  //println("dfWithColumns schema:")
  //dfWithColumns.printSchema()

  val data = dfWithColumns
    .withColumn("current_time", current_timestamp())
    .withColumn("comment_id", dfWithColumns.col("value.id"))
    .withColumn("video_id", dfWithColumns.col("value.snippet.videoId"))
    .withColumn("comment_text", dfWithColumns.col("value.snippet.topLevelComment.snippet.textDisplay"))
    .withColumn("comment_author_channel_name", dfWithColumns.col("value.snippet.topLevelComment.snippet.authorDisplayName"))
    .withColumn("comment_author_channel_id", dfWithColumns.col("value.snippet.topLevelComment.snippet.authorChannelId.value"))
    .withColumn("comment_author_channel_url", dfWithColumns.col("value.snippet.topLevelComment.snippet.authorChannelUrl"))
    .withColumn("published_at", dfWithColumns.col("value.snippet.topLevelComment.snippet.publishedAt").cast(TimestampType))
    .withColumn("comment_like_count", dfWithColumns.col("value.snippet.topLevelComment.snippet.likeCount"))
    .withColumn("total_reply_count", dfWithColumns.col("value.snippet.totalReplyCount"))
    .drop("value")

  //enrich the comments table

  import org.apache.spark.sql.functions.udf

  val countWordsFunction: UserDefinedFunction = udf((text: String) => {
    val cleanText = text.replaceAll("""[\p{Punct}]""", "")
    val words = cleanText.split(" ")
    words.length
  })

  val enrichedDf = data
    .withColumn("text_length", countWordsFunction(col("comment_text")))

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

  def streamFromKafkaToDruid(topicDruid: String): Unit = {

    enrichedDf.select(to_json(struct("*")).alias("value"))
    enrichedDf.selectExpr("to_json(struct(*)) AS value")
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
          .option("dbtable", s"public.yt_comments")
          .option("user", "postgres")
          .option("password", "FoolishPassword")
          .mode(mode)
          .save()
      }
      .start()
      .awaitTermination()
  }

}
