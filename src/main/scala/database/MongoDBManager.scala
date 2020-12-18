package database

import org.apache.spark.sql.{DataFrame, SaveMode}
import com.mongodb.spark._
import app.SparkApp

object MongoDBManager extends SparkApp {

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val df = Seq(
      (1, "one"),
      (2, "two")).toDF("num", "string_num")

    //val mongoMngr = new MongoDBManager()
    MongoDBManager.writeDf(df, "", "", SaveMode.Append)
  }

  def writeDf(df: DataFrame, db: String, table: String, mode: SaveMode): Unit = {
    df.write.format("mongo")
      .option("uri", "mongodb://localhost:27017/")
      .option("database", "admin")
      .option("collection", "test")
      .mode(mode)
      .save()
  }
}
