package utils

import org.apache.spark.sql.SparkSession

object Spark {
  def getSparkSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }
}
