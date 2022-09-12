package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

class PlaylistsEnricher {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configSpark.getString("checkpointLocation")

  val configPostgres: Config = ConfigFactory.load().getConfig("application.postgres")
  val postgresDriver: String = configPostgres.getString("driver")
  val postgresUrl: String = configPostgres.getString("url")
  val postgresDbtablePlaylists: String = configPostgres.getString("dbtablePlaylists")
  val postgresDbtablePlaylistsEnriched: String = configPostgres.getString("dbtablePlaylistsEnriched")
  val postgresUser: String = configPostgres.getString("user")
  val postgresPassword: String = configPostgres.getString("password")

  lazy val spark = SparkSession
    .builder()
    .config("spark.speculation", "false")
    .config("checkpointLocation", s"$checkpoint")
    .master(s"$sparkCores")
    .appName("enrich playlists table")
    .getOrCreate()

  LoggerFactory.getLogger(spark.getClass)
  spark.sparkContext.setLogLevel("WARN")

  def enrichPlaylistsTable(): Unit = {

    //get playlists table from Postgres
    val playlistsDf = spark.read
      .format("jdbc")
      .option("driver", s"$postgresDriver")
      .option("url", s"$postgresUrl")
      .option("dbtable", s"$postgresDbtablePlaylists")
      .option("user", s"$postgresUser")
      .option("password", s"$postgresPassword")
      .load()

    //enrich the playlists table
    import org.apache.spark.sql.functions.udf
    val countWordsFunction: UserDefinedFunction = udf((text: String) => {
      val cleanText = text.replaceAll("""[\p{Punct}]""", "")
      val words = cleanText.split(" ")
      words.length
    })
    val withNumOfWordsDf = playlistsDf
      .withColumn("playlist_title_length", countWordsFunction(col("playlist_title")))

    val mode: SaveMode = SaveMode.ErrorIfExists

    // write new batch table to Postgres
    withNumOfWordsDf.write
      .format("jdbc")
      .option("driver", s"$postgresDriver")
      .option("url", s"$postgresUrl")
      .option("dbtable", s"$postgresDbtablePlaylistsEnriched")
      .option("user", s"$postgresUser")
      .option("password", s"$postgresPassword")
      .mode(mode)
      .save()
  }


}
