package exercises.chapter2

import org.apache.spark.sql.{SparkSession, functions => F}

object MnMCount {
  def run(spark: SparkSession): Unit = {
    val path = "data/chapter2/mnm_dataset.csv"

    val mnmDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

    val countMnMDF = mnmDF
      .groupBy("State", "Color")
      .agg(F.sum("Count").as("Total"))
      .orderBy(F.desc("Total"))

    countMnMDF.show(60, truncate = false)
  }
}
