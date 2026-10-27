package lab.spark

import org.apache.spark.sql.SparkSession

object EnvironmentCheck {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("scala-environment-check").getOrCreate()
    try {
      val rows = spark.read
        .option("header", "true")
        .csv("s3a://raw/orders/2026-10-26/orders.csv")
        .count()
      println(s"Scala ${util.Properties.versionNumberString}, Spark ${spark.version}, rows=$rows")
      require(rows == 7L, s"Expected 7 input rows, got $rows")
    } finally {
      spark.stop()
    }
  }
}
