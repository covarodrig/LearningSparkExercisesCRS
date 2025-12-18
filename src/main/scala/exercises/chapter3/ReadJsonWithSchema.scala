package exercises.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object ReadJsonWithSchema {

  def run(spark: SparkSession): Unit = {
    val jsonFile = "data/chapter3/blogs.json" // ajusta si tu nombre es otro

    val schema = StructType(Array(
      StructField("Id", IntegerType, nullable = false),
      StructField("First", StringType, nullable = false),
      StructField("Last", StringType, nullable = false),
      StructField("Url", StringType, nullable = false),
      StructField("Published", StringType, nullable = false),
      StructField("Hits", IntegerType, nullable = false),
      StructField("Campaigns", ArrayType(StringType), nullable = false)
    ))

    val blogsDF = spark.read.schema(schema).json(jsonFile)

    blogsDF.show(truncate = false)
    blogsDF.printSchema()
  }
}
