package exercises.chapter3

import org.apache.spark.sql.SparkSession

object Chapter3Runner {
  def run(spark: SparkSession): Unit = {
    ReadJsonWithSchema.run(spark)
    SFFireCalls.run(spark)
    IoTDevices.run(spark)
  }
}
