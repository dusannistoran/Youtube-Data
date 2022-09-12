package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.slf4j.LoggerFactory

class PlaylistsUpdates(topicNifi: String, broker: String) {

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
          StructField("channelId", StringType) ::
          StructField("channelTitle", StringType) :: Nil
      )) ::
        StructField("contentDetails", StructType(
          StructField("itemCount", IntegerType) :: Nil
        )) :: Nil
  )

  val dfWithColumns = valueDf
    .withColumn("value", from_json(col("value"), mySchema))

  val data = dfWithColumns
    .withColumn("current_time", current_timestamp())
    .withColumn("playlist_id", dfWithColumns.col("value.id"))
    .withColumn("playlist_title", dfWithColumns.col("value.snippet.title"))
    .withColumn("playlist_description", dfWithColumns.col("value.snippet.description"))
    .withColumn("published_at", dfWithColumns.col("value.snippet.publishedAt").cast(TimestampType))
    .withColumn("channel_id", dfWithColumns.col("value.snippet.channelId"))
    .withColumn("channel_title", dfWithColumns.col("value.snippet.channelTitle"))
    .withColumn("videos_count", dfWithColumns.col("value.contentDetails.itemCount"))
    .drop("value")

  def streamFromKafkaToConsole(): Unit = {

    println("data schema:")
    data.printSchema()

    //enrich the playlists table
    import org.apache.spark.sql.functions.udf
    val countWordsFunction: UserDefinedFunction = udf((text: String) => {
      val cleanText = text.replaceAll("""[\p{Punct}]""", "")
      val words = cleanText.split(" ")
      words.length
    })
    val withNumOfWordsDf = data
      .withColumn("playlist_title_length", countWordsFunction(col("playlist_title")))

    withNumOfWordsDf.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

  def streamFromKafkaToDruid(topicDruid: String): Unit = {

    //enrich the playlists table
    import org.apache.spark.sql.functions.udf
    val countWordsFunction: UserDefinedFunction = udf((text: String) => {
      val cleanText = text.replaceAll("""[\p{Punct}]""", "")
      val words = cleanText.split(" ")
      words.length
    })

    val enrichedDf = data
      .withColumn("playlist_title_length", countWordsFunction(col("playlist_title")))

    // send to Kafka
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

  /*
  def streamFromKafkaToPostgres(today: String): Unit = {

    println("data schema:")
    data.printSchema()

    //enrich the playlists table
    import org.apache.spark.sql.functions.udf
    val countWordsFunction: UserDefinedFunction = udf((text: String) => {
      val cleanText = text.replaceAll("""[\p{Punct}]""", "")
      val words = cleanText.split(" ")
      words.length
    })
    val withNumOfWordsDf = data
      .withColumn("playlist_title_length", countWordsFunction(col("playlist_title")))

    val mode: SaveMode = SaveMode.Append
    withNumOfWordsDf.writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        batch.write
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", "jdbc:postgresql://postgres_db:5432/youtube_updates")
          .option("dbtable", s"public.yt_playlists_$today")
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
