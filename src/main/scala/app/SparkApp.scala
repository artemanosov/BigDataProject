package app

import org.apache.spark.sql.SparkSession

trait SparkApp {
  implicit val spark: SparkSession = SparkSession.builder()//.enableHiveSupport()
    .appName(this.getClass.getSimpleName)
    .config("spark.master", "local")
    .config("spark.executer.extraJavaOptions", "-XX:+UseCompressedOops")
    //.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/admin.test")
    //.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/admin.test")
    .config("spark.testing.memory", "512000000")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
}
