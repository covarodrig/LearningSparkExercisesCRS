package exercises.chapter3

import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.types._

object SFFireCalls {

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    val fireSchema = StructType(Array(
      StructField("CallNumber", IntegerType, true),
      StructField("UnitID", StringType, true),
      StructField("IncidentNumber", IntegerType, true),
      StructField("CallType", StringType, true),
      StructField("CallDate", StringType, true),
      StructField("WatchDate", StringType, true),
      StructField("CallFinalDisposition", StringType, true),
      StructField("AvailableDtTm", StringType, true),
      StructField("Address", StringType, true),
      StructField("City", StringType, true),
      StructField("Zipcode", IntegerType, true),
      StructField("Battalion", StringType, true),
      StructField("StationArea", StringType, true),
      StructField("Box", StringType, true),
      StructField("OriginalPriority", StringType, true),
      StructField("Priority", StringType, true),
      StructField("FinalPriority", IntegerType, true),
      StructField("ALSUnit", BooleanType, true),
      StructField("CallTypeGroup", StringType, true),
      StructField("NumAlarms", IntegerType, true),
      StructField("UnitType", StringType, true),
      StructField("UnitSequenceInCallDispatch", IntegerType, true),
      StructField("FirePreventionDistrict", StringType, true),
      StructField("SupervisorDistrict", StringType, true),
      StructField("Neighborhood", StringType, true),
      StructField("Location", StringType, true),
      StructField("RowID", StringType, true),
      StructField("Delay", FloatType, true)
    ))

    val sfFireFile = "data/chapter3/sf-fire-calls.csv"

    val fireDF = spark.read
      .schema(fireSchema)
      .option("header", "true")
      .csv(sfFireFile)

    println("=== Sample Data ===")
    fireDF.show(5, truncate = false)

    println("\n=== Projection and Filter: Non-Medical Incidents ===")
    fireDF
      .select("IncidentNumber", "AvailableDtTm", "CallType")
      .where($"CallType" =!= "Medical Incident")
      .show(5, truncate = false)

    println("\n=== Distinct Call Types ===")
    fireDF
      .select("CallType")
      .where($"CallType".isNotNull)
      .agg(F.countDistinct("CallType").as("DistinctCallTypes"))
      .show()

    println("\n=== List of Distinct Call Types ===")
    fireDF
      .select("CallType")
      .where($"CallType".isNotNull)
      .distinct()
      .show(10, truncate = false)

    println("\n=== Response Delays > 5 minutes ===")
    val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
    newFireDF
      .select("ResponseDelayedinMins")
      .where($"ResponseDelayedinMins" > 5)
      .show(5, truncate = false)

    println("\n=== Converting dates to timestamps ===")
    val fireTsDF = newFireDF
      .withColumn("IncidentDate", F.to_timestamp($"CallDate", "MM/dd/yyyy"))
      .drop("CallDate")
      .withColumn("OnWatchDate", F.to_timestamp($"WatchDate", "MM/dd/yyyy"))
      .drop("WatchDate")
      .withColumn("AvailableDtTS", F.to_timestamp($"AvailableDtTm", "MM/dd/yyyy hh:mm:ss a"))
      .drop("AvailableDtTm")

    fireTsDF.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, truncate = false)

    println("\n=== Years covered in dataset ===")
    fireTsDF
      .select(F.year($"IncidentDate").as("Year"))
      .distinct()
      .orderBy($"Year")
      .show()

    println("\n=== Most Common Call Types ===")
    fireTsDF
      .select("CallType")
      .where($"CallType".isNotNull)
      .groupBy("CallType")
      .count()
      .orderBy(F.desc("count"))
      .show(10, truncate = false)

    println("\n=== Statistical Summary ===")
    fireTsDF
      .select(
        F.sum("NumAlarms").as("SumNumAlarms"),
        F.avg("ResponseDelayedinMins").as("AvgDelay"),
        F.min("ResponseDelayedinMins").as("MinDelay"),
        F.max("ResponseDelayedinMins").as("MaxDelay")
      )
      .show()
  }
}

