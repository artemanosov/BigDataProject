package database

import java.sql.{Connection, DriverManager}
import java.util.Properties
import app.SparkApp

import org.apache.spark.sql.{DataFrame, SaveMode}

object PostgresDBManager extends SparkApp {

  val connectionProperties = new Properties()
  var connection = None: Option[Connection]

  def main(args: Array[String]): Unit = {
    println("Starting PostgresSQL DB Manager...")

    PostgresDBManager.setup()
    PostgresDBManager.run()
  }

  def setup(): Unit = {
    println("Setting up PostgresSQL connection...")
    val url = "jdbc:postgresql://localhost:5432/test"

    connection = Some(DriverManager.getConnection(url, "postgres", "password"))

    //deprecated
    connectionProperties.put("user", "postgres")
    connectionProperties.put("password", "password")
    connectionProperties.put("driver", "org.postgresql.Driver")
  }

  def shutDown(): Unit = {
    if(connection.isDefined) connection.get.close()
    else print("Connection was not established. Nothing to shut down!")
  }

  def run(): Unit = ???

  def query(query: String): DataFrame = {
    spark.read.format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/test")
      .option("user", "postgres")
      .option("password", "password")
      .option("driver", "org.postgresql.Driver")
      .option("query", query)
      .load()
  }

  def readDf(database: String, table: String): DataFrame = {
    spark.read.format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/${database}")
      .option("dbtable", table)
      .option("user", "postgres")
      .option("password", "password")
      .option("driver", "org.postgresql.Driver")
      .load()
  }

  def writeDf(df: DataFrame, db: String, table: String, mode: SaveMode): Unit = {
    df.write.format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:5432/${db}")
      .option("dbtable", table)
      .option("user", "postgres")
      .option("password", "password")
      .option("driver", "org.postgresql.Driver")
      .mode(mode)
      .save()
  }
}
