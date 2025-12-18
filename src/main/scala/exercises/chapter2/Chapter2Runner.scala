package exercises.chapter2

import org.apache.spark.sql.SparkSession

object Chapter2Runner {
  def run(spark: SparkSession): Unit = {
    MnMCount.run(spark)
  }
}
