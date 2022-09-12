package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.slf4j.LoggerFactory

class VideosConsumer(topicNifi: String, broker: String) {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configSpark.getString("checkpointLocation")

  lazy val spark = SparkSession
    .builder()
    .config("spark.speculation", "false")
    .config("checkpointLocation", s"$checkpoint")
    .master(s"$sparkCores")
    .appName("consume videos stream to console and to druid")
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

  val videosSchema = StructType(
      StructField("video_id", StringType) ::
      StructField("published_at", TimestampType) ::
        StructField("channel_id", StringType) ::
        StructField("channel_title", StringType) ::
        StructField("video_title", StringType) ::
        StructField("video_description", StringType) ::
        StructField("view_count", LongType) ::
        StructField("like_count", IntegerType) ::
        StructField("comment_count", IntegerType) ::
        StructField("category_id", IntegerType) ::
        StructField("category", StringType) ::
        StructField("language", StringType) :: Nil
  )

  val valueDf = df.selectExpr("CAST(value AS STRING)")

  import org.apache.spark.sql.functions._

  val dfWithColumns = valueDf
    .withColumn("value", from_json(col("value"), videosSchema))

  val data = dfWithColumns
    .withColumn("video_id", dfWithColumns.col("value.video_id"))
    .withColumn("published_at", dfWithColumns.col("value.published_at"))
    .withColumn("channel_id", dfWithColumns.col("value.channel_id"))
    .withColumn("channel_title", dfWithColumns.col("value.channel_title"))
    .withColumn("video_title", dfWithColumns.col("value.video_title"))
    .withColumn("video_description", dfWithColumns.col("value.video_description"))
    .withColumn("view_count", dfWithColumns.col("value.view_count"))
    .withColumn("like_count", dfWithColumns.col("value.like_count"))
    .withColumn("comment_count", dfWithColumns.col("value.comment_count"))
    .withColumn("category_id", dfWithColumns.col("value.category_id"))
    .withColumn("category", dfWithColumns.col("value.category"))
    .withColumn("language", dfWithColumns.col("value.language"))
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
    val countFunction: UserDefinedFunction = udf((word: String, text: String) => {
      val cleanText = text.replaceAll("""[\p{Punct}]""", "").toLowerCase()
      val words = cleanText.split(" ")
      words.count(_.equals(word))
    })

    val dataEnriched = data
      .withColumn("facebook", lit("facebook"))
      .withColumn("instagram", lit("instagram"))
      .withColumn("twitter", lit("twitter"))
      .withColumn("youtube", lit("youtube"))
      .withColumn("tiktok", lit("tiktok"))
      .withColumn("reddit", lit("reddit"))
      .withColumn("discord", lit("discord"))

    val withSocialNetworks = dataEnriched
      .withColumn("facebook_count", countFunction(col("facebook"), col("video_description")))
      .withColumn("instagram_count", countFunction(col("instagram"), col("video_description")))
      .withColumn("twitter_count", countFunction(col("twitter"), col("video_description")))
      .withColumn("youtube_count", countFunction(col("youtube"), col("video_description")))
      .withColumn("tiktok_count", countFunction(col("tiktok"), col("video_description")))
      .withColumn("reddit_count", countFunction(col("reddit"), col("video_description")))
      .withColumn("discord_count", countFunction(col("discord"), col("video_description")))
      .drop("facebook")
      .drop("instagram")
      .drop("twitter")
      .drop("youtube")
      .drop("tiktok")
      .drop("reddit")
      .drop("discord")

    println("withSocialNetworks schema:")
    withSocialNetworks.printSchema()

    withSocialNetworks.selectExpr("CAST(published_at AS STRING) AS key", "to_json(struct(*)) AS value")
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
