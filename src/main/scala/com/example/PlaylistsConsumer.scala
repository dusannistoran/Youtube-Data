package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.slf4j.LoggerFactory

class PlaylistsConsumer(topicNifi: String, broker: String) {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configSpark.getString("checkpointLocation")

  lazy val spark = SparkSession
    .builder()
    .config("spark.speculation", "false")
    .config("checkpointLocation", s"$checkpoint")
    .master(s"$sparkCores")
    .appName("consume playlists stream to console and to druid")
    .getOrCreate()

  LoggerFactory.getLogger(spark.getClass)
  spark.sparkContext.setLogLevel("WARN")

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", broker)
    .option("subscribe", topicNifi)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()

  val playlistsSchema = StructType(
    StructField("playlist_id", StringType) ::
      StructField("playlist_title", StringType) ::
      StructField("playlist_description", StringType) ::
      StructField("published_at", TimestampType) ::
      StructField("channel_id", StringType) ::
      StructField("channel_title", StringType) ::
      StructField("videos_count", IntegerType) :: Nil
  )

  val valueDf = df.selectExpr("CAST(value AS STRING)")

  import org.apache.spark.sql.functions._

  val dfWithColumns = valueDf
    .withColumn("value", from_json(col("value"), playlistsSchema))

  val data = dfWithColumns
    .withColumn("playlist_id", dfWithColumns.col("value.playlist_id"))
    .withColumn("playlist_title", dfWithColumns.col("value.playlist_title"))
    .withColumn("playlist_description", dfWithColumns.col("value.playlist_description"))
    .withColumn("published_at", dfWithColumns.col("value.published_at"))
    .withColumn("channel_id", dfWithColumns.col("value.channel_id"))
    .withColumn("channel_title", dfWithColumns.col("value.channel_title"))
    .withColumn("videos_count", dfWithColumns.col("value.videos_count"))
    .drop("value")

  def streamFromKafkaToConsole(): Unit = {

    println("data schema:")
    data.printSchema()

    data.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "true")
      .start()
      .awaitTermination()
  }

  def streamFromKafkaToDruid(topicDruid: String): Unit = {
    println("data schema:")
    data.printSchema()

    import org.apache.spark.sql.functions.udf


    val countWordsFunction: UserDefinedFunction = udf((text: String) => {
      val cleanText = text.replaceAll("""[\p{Punct}]""", "")
      val words = cleanText.split(" ")
      words.length
    })

    val withNumOfWords: DataFrame = data
      .withColumn("playlist_title_length", countWordsFunction(col("playlist_title")))

    println("withNumOfWords schema:")
    withNumOfWords.printSchema()

    withNumOfWords.selectExpr("CAST(published_at AS STRING) AS key", "to_json(struct(*)) AS value")
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
