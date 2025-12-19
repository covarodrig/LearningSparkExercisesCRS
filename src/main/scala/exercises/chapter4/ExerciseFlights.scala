package exercises.chapter4

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ExerciseFlights {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Chapter4 - ExerciseFlights")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    doExerciseFlights()

    spark.stop()
  }

  def doExerciseFlights()(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    // 1) Load dataset (CSV) + schema handling
    val path = "src/main/resources/chapter4/departuredelays.csv"

    val flightsRaw = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)

    println("=== Raw flights schema ===")
    flightsRaw.printSchema()
    flightsRaw.show(5, truncate = false)

    // 2) Normalize columns + build a real timestamp from the "date" string
    // Original format: MMDDHHMM (e.g., 02190925 -> Feb 19 09:25)
    // We add a fixed year (e.g., 2010) to make it parseable.
    val flights = flightsRaw
      .select($"date", $"delay", $"distance", $"origin", $"destination")
      .withColumn("date_str", lpad(col("date").cast("string"), 8, "0"))
      .withColumn(
        "flight_ts",
        to_timestamp(
          concat(lit("2010"), col("date_str")),
          "yyyyMMddHHmm"
        )
      )
      .drop("date_str")

    println("=== Normalized flights (with flight_ts) ===")
    flights.select("date", "flight_ts", "delay", "distance", "origin", "destination")
      .show(10, truncate = false)

    // 3) Temporary view for Spark SQL
    flights.createOrReplaceTempView("us_delay_flights_tbl")

    // 4) Spark SQL queries (as in the book)
    println("=== Q1: distance > 1000 ===")
    spark.sql("""
      SELECT distance, origin, destination
      FROM us_delay_flights_tbl
      WHERE distance > 1000
      ORDER BY distance DESC
    """).show(10, truncate = false)

    println("=== Q2: SFO -> ORD with delay > 120 ===")
    spark.sql("""
      SELECT date, flight_ts, delay, origin, destination
      FROM us_delay_flights_tbl
      WHERE delay > 120 AND origin = 'SFO' AND destination = 'ORD'
      ORDER BY delay DESC
    """).show(10, truncate = false)

    println("=== Q3: CASE label delays ===")
    spark.sql("""
      SELECT delay, origin, destination,
        CASE
          WHEN delay > 360 THEN 'Very Long Delays'
          WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
          WHEN delay > 60  AND delay < 120 THEN 'Short Delays'
          WHEN delay > 0   AND delay < 60  THEN 'Tolerable Delays'
          WHEN delay = 0 THEN 'No Delays'
          ELSE 'Early'
        END AS Flight_Delays
      FROM us_delay_flights_tbl
      ORDER BY origin, delay DESC
    """).show(10, truncate = false)

    // 5) DataFrame API equivalent for at least one query (chapter suggests practicing interoperability)
    println("=== DataFrame API equivalent (distance > 1000) ===")
    flights
      .select($"distance", $"origin", $"destination")
      .where($"distance" > 1000)
      .orderBy(desc("distance"))
      .show(10, truncate = false)

    // 6) Optional: read your outputs from chapter3 (keep if it exists in your project)
    // NOTE: Spark expects folder paths for DataFrameWriter outputs (multiple part files).
    // Adjust paths if needed.
    println("=== Read outputs created in chapter3 (if present) ===")

    safeShow("CSV fireCalls", () =>
      spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("src/main/resources/output/csv/fireCalls")
        .show(5, truncate = false)
    )

    safeShow("JSON fireCallsDate", () =>
      spark.read.format("json")
        .load("src/main/resources/output/json/fireCallsDate")
        .show(5, truncate = false)
    )

    safeShow("Parquet commonCalls", () =>
      spark.read.format("parquet")
        .load("src/main/resources/output/parquet/commonCalls")
        .show(5, truncate = false)
    )
  }

  /** Helper to avoid the whole exercise failing if optional files aren't present yet. */
  private def safeShow(label: String, action: () => Unit): Unit = {
    try {
      println(s"--- $label ---")
      action()
    } catch {
      case e: Exception =>
        println(s"[SKIP] Could not read '$label' due to: ${e.getMessage}")
    }
  }
}


