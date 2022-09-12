package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit}
import org.slf4j.LoggerFactory

class VideosEnricher {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configSpark.getString("checkpointLocation")

  val configPostgres: Config = ConfigFactory.load().getConfig("application.postgres")
  val postgresDriver: String = configPostgres.getString("driver")
  val postgresUrl: String = configPostgres.getString("url")
  val postgresDbtableVideos: String = configPostgres.getString("dbtableVideos")
  val postgresDbtableVideosEnriched: String = configPostgres.getString("dbtableVideosEnriched")
  val postgresUser: String = configPostgres.getString("user")
  val postgresPassword: String = configPostgres.getString("password")

  lazy val spark = SparkSession
    .builder()
    .config("spark.speculation", "false")
    .config("checkpointLocation", s"$checkpoint")
    .master(s"$sparkCores")
    .appName("enrich videos table")
    .getOrCreate()

  LoggerFactory.getLogger(spark.getClass)
  spark.sparkContext.setLogLevel("WARN")

  def enrichVideosTable(): Unit = {

    //get videos table from Postgres
    val videosDf = spark.read
      .format("jdbc")
      .option("driver", s"$postgresDriver")
      .option("url", s"$postgresUrl")
      .option("dbtable", s"$postgresDbtableVideos")
      .option("user", s"$postgresUser")
      .option("password", s"$postgresPassword")
      .load()

    //enrich the videos table
    import org.apache.spark.sql.functions.udf
    val countFunction: UserDefinedFunction = udf((word: String, text: String) => {
      val cleanText = text.replaceAll("""[\p{Punct}]""", "").toLowerCase()
      val words = cleanText.split(" ")
      words.count(_.equals(word))
    })

    val videosEnrichedDf = videosDf
      .withColumn("facebook", lit("facebook"))
      .withColumn("instagram", lit("instagram"))
      .withColumn("twitter", lit("twitter"))
      .withColumn("youtube", lit("youtube"))
      .withColumn("tiktok", lit("tiktok"))
      .withColumn("reddit", lit("reddit"))
      .withColumn("discord", lit("discord"))

    val videosWithSocialNetworksDf = videosEnrichedDf
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

    val mode: SaveMode = SaveMode.ErrorIfExists

    // write new table to Postgres
    videosWithSocialNetworksDf.write
      .format("jdbc")
      .option("driver", s"$postgresDriver")
      .option("url", s"$postgresUrl")
      .option("dbtable", s"$postgresDbtableVideosEnriched")
      .option("user", s"$postgresUser")
      .option("password", s"$postgresPassword")
      .mode(mode)
      .save()
  }

}
