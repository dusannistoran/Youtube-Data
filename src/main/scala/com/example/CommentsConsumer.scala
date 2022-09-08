package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.slf4j.LoggerFactory

class CommentsConsumer(topicNifi: String, broker: String) {

  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val sparkCores: String = configSpark.getString("master")
  //val HDFS_HOME: String = configSpark.getString("hdfsHome")
  val checkpoint: String = configSpark.getString("checkpointLocation")

  lazy val spark = SparkSession
    .builder()
    .config("spark.speculation", "false")
    .config("checkpointLocation", s"$checkpoint")
    //.config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .master(s"$sparkCores")
    .appName("consume comments stream to console")
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

  val commentsSchema = StructType(
    StructField("comment_id", StringType) ::
    StructField("video_id", StringType) ::
      StructField("comment_text", StringType) ::
      StructField("comment_author_channel_name", StringType) ::
      StructField("comment_author_channel_id", StringType) ::
      StructField("comment_like_count", IntegerType) ::
      //StructField("published_at", StringType) ::
      StructField("published_at_timestamp", TimestampType) ::
      StructField("total_reply_count", IntegerType) :: Nil
  )

  val valueDf = df.selectExpr("CAST(value AS STRING)")
  import org.apache.spark.sql.functions._
  val dfWithColumns = valueDf
    .withColumn("value", from_json(col("value"), commentsSchema))

  val data = dfWithColumns
    .withColumn("comment_id", dfWithColumns.col("value.comment_id"))
    .withColumn("video_id", dfWithColumns.col("value.video_id"))
    .withColumn("comment_text", dfWithColumns.col("value.comment_text"))
    .withColumn("comment_author_channel_name", dfWithColumns.col("value.comment_author_channel_name"))
    .withColumn("comment_author_channel_id", dfWithColumns.col("value.comment_author_channel_id"))
    .withColumn("comment_like_count", dfWithColumns.col("value.comment_like_count"))
    //.withColumn("published_at", dfWithColumns.col("value.published_at"))
    .withColumn("published_at", dfWithColumns.col("value.published_at_timestamp"))
    .withColumn("total_reply_count", dfWithColumns.col("value.total_reply_count"))
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
      .withColumn("text_length", countWordsFunction(col("comment_text")))

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
