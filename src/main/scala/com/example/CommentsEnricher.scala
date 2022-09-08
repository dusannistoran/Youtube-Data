package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

class CommentsEnricher {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val sparkCores: String = configSpark.getString("master")
  val checkpoint: String = configSpark.getString("checkpointLocation")

  val configPostgres: Config = ConfigFactory.load().getConfig("application.postgres")
  val postgresDriver: String = configPostgres.getString("driver")
  val postgresUrl: String = configPostgres.getString("url")
  val postgresDbtableComments: String = configPostgres.getString("dbtableComments")
  val postgresDbtableCommentsEnriched: String = configPostgres.getString("dbtableCommentsEnriched")
  val postgresUser: String = configPostgres.getString("user")
  val postgresPassword: String = configPostgres.getString("password")

  lazy val spark = SparkSession
    .builder()
    .config("spark.speculation", "false")
    .config("checkpointLocation", s"$checkpoint")
    .master(s"$sparkCores")
    .appName("enrich comments table")
    .getOrCreate()

  LoggerFactory.getLogger(spark.getClass)
  spark.sparkContext.setLogLevel("WARN")

  def enrichCommentsTable(): Unit = {

    //get comments table from Postgres
    val commentsDf = spark.read
      .format("jdbc")
      .option("driver", s"$postgresDriver")
      .option("url", s"$postgresUrl")
      .option("dbtable", "youtube_comments")
      .option("user", s"$postgresUser")
      .option("password", s"$postgresPassword")
      .load()

    commentsDf.printSchema()
    println("There are " + commentsDf.count() + " rows in commentsDf.")
    commentsDf.show()

    //enrich the comments table
    import org.apache.spark.sql.functions.udf
    val countWordsFunction: UserDefinedFunction = udf((text: String) => {
      val cleanText = text.replaceAll("""[\p{Punct}]""", "")
      val words = cleanText.split(" ")
      words.length
    })

    val commentsEnrichedDf = commentsDf
      .withColumn("text_length", countWordsFunction(col("comment_text")))

    val mode: SaveMode = SaveMode.ErrorIfExists

    // write new table to Postgres
    commentsEnrichedDf.write
      .format("jdbc")
      .option("driver", s"$postgresDriver")
      .option("url", s"$postgresUrl")
      .option("dbtable", "yt_comments_enriched_1")
      .option("user", s"$postgresUser")
      .option("password", s"$postgresPassword")
      .mode(mode)
      .save()
  }

}
