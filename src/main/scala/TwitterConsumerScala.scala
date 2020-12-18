import app.SparkApp
import database.{MongoDBManager, PostgresDBManager}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.slf4j.LoggerFactory
import schema.{ExtendedTweet, Tweet}

object TwitterConsumerScala extends SparkApp {
  final val logger = LoggerFactory.getLogger(TwitterConsumerScala.getClass)

  def main(args: Array[String]): Unit = {
    val producerThread = new Thread(TwitterProducerScala)
    producerThread.start()
    TwitterConsumerScala.run()
  }

  def run(): Unit = {
    import spark.implicits._

    val schema = ScalaReflection.schemaFor[ExtendedTweet].dataType.asInstanceOf[StructType]
    PostgresDBManager.setup()

    val msg = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092") //add port
      .option("subscribe", "twitter_tweets")
      //.option("startingOffsets", "latest")
      .load()

    //tweets.printSchema()

    val tweets = msg.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).as("tweet"))
      //.select($"tweet.id",  $"tweet.user.name".as("user"), $"tweet.text", $"tweet.created_at")

    tweets.printSchema()

    val database = "admin"
    val table = "test"

    tweets.writeStream.foreachBatch((df: Dataset[Row], id: Long) => {
      df.withColumn("processed_at", current_timestamp())

      MongoDBManager.writeDf(df, database, table, SaveMode.Append)
    }).start().awaitTermination()

    /*
    tweets.writeStream.foreachBatch((df: Dataset[Row], id: Long) => {
      df
        .withColumn("id", lit(id))
        .withColumn("processed_at", current_timestamp())
        .as[Tweet]

      dbManager.writeDf(df, database, table, SaveMode.Append)
    }).start().awaitTermination()*/
  }
}
