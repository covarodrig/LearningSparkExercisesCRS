package exercises.chapter5

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object RelationalOps {

  def run(spark: SparkSession): Unit = {
    println("\n==============================")
    println("CHAPTER 5 - Relational Ops")
    println("Unions, Joins, Windowing, Modifications, Pivot")
    println("==============================\n")

    // 1) Preparación de datos
    val (airports, delays, foo) = prepareData(spark)

    // 2) UNIONS
    unionsExample(spark, delays, foo)

    // 3) JOINS
    joinsExample(spark, airports, foo)

    // 4) WINDOWING (dense_rank top 3 por origin)
    windowingExample(spark, delays)

    // 5) MODIFICATIONS (withColumn, drop, rename)
    modificationsExample(foo)

    // 6) PIVOT (SQL pivot como el libro + alternativa DataFrame)
    pivotExample(spark)
  }

  // -----------------------------
  // 1) Importar ficheros + cast + foo
  // -----------------------------
  private def prepareData(spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {
    import spark.implicits._

    // Rutas dentro de src/main/resources
    val delaysPath   = resourcePath("/data/chapter5/departuredelays.csv")
    val airportsPath = resourcePath("/data/chapter5/airport-codes-na.txt")

    println(s"Using delaysPath   = $delaysPath")
    println(s"Using airportsPath = $airportsPath")

    // (1) Airports (airports_na)
    val airports = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .csv(airportsPath)

    airports.createOrReplaceTempView("airports_na")

    // (2) Departure delays (departureDelays) + CAST delay/distance
    val delaysRaw = spark.read
      .option("header", "true")
      .csv(delaysPath)

    val delays = delaysRaw
      .withColumn("delay", expr("CAST(delay AS INT)"))
      .withColumn("distance", expr("CAST(distance AS INT)"))

    delays.createOrReplaceTempView("departureDelays")

    // (3) foo: SEA -> SFO, date like '01010%' y delay > 0
    val foo = delays.filter(
      expr("""
        origin = 'SEA' AND destination = 'SFO'
        AND date LIKE '01010%'
        AND delay > 0
      """)
    )
    foo.createOrReplaceTempView("foo")

    // Verificaciones (como en el libro)
    println("\n--- airports_na (LIMIT 10) ---")
    spark.sql("SELECT * FROM airports_na LIMIT 10").show(false)

    println("\n--- departureDelays (LIMIT 10) ---")
    spark.sql("SELECT * FROM departureDelays LIMIT 10").show(false)

    println("\n--- foo ---")
    spark.sql("SELECT * FROM foo").show(false)

    (airports, delays, foo)
  }

  private def resourcePath(resource: String): String = {
    val url = Option(getClass.getResource(resource))
      .getOrElse(throw new IllegalArgumentException(
        s"Resource not found: $resource. " +
          s"Verify it exists under src/main/resources."
      ))
    url.getPath
  }

  // -----------------------------
  // 2) UNIONS
  // -----------------------------
  private def unionsExample(spark: SparkSession, delays: DataFrame, foo: DataFrame): Unit = {
    println("\n==============================")
    println("2) UNIONS")
    println("==============================")

    val bar = delays.union(foo)
    bar.createOrReplaceTempView("bar")

    // DataFrame API (como el libro)
    println("\n--- bar filtered (DataFrame API) ---")
    bar.filter(expr("""
      origin = 'SEA' AND destination = 'SFO'
      AND date LIKE '01010%'
      AND delay > 0
    """)).show(false)

    // SQL (como el libro)
    println("\n--- bar filtered (SQL) ---")
    spark.sql("""
      SELECT *
      FROM bar
      WHERE origin = 'SEA'
        AND destination = 'SFO'
        AND date LIKE '01010%'
        AND delay > 0
    """).show(false)
  }

  // -----------------------------
  // 3) JOINS
  // -----------------------------
  private def joinsExample(spark: SparkSession, airports: DataFrame, foo: DataFrame): Unit = {
    println("\n==============================")
    println("3) JOINS (inner join)")
    println("==============================")

    // DataFrame join (inner por defecto)
    println("\n--- Join foo with airports_na (DataFrame API) ---")
    foo.join(
      airports.as("air"),
      col("air.IATA") === col("origin"),
      "inner"
    ).select(
      col("air.City"),
      col("air.State"),
      col("date"),
      col("delay"),
      col("distance"),
      col("destination")
    ).show(false)

    // SQL join (como el libro)
    println("\n--- Join foo with airports_na (SQL) ---")
    spark.sql("""
      SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination
      FROM foo f
      JOIN airports_na a
        ON a.IATA = f.origin
    """).show(false)
  }

  // -----------------------------
  // 4) WINDOWING (dense_rank)
  // -----------------------------
  private def windowingExample(spark: SparkSession, delays: DataFrame): Unit = {
    println("\n==============================")
    println("4) WINDOWING (dense_rank top 3 destinations by origin)")
    println("==============================")

    // Creamos el equivalente a departureDelaysWindow:
    val departureDelaysWindow = delays
      .filter(
        col("origin").isin("SEA", "SFO", "JFK") &&
          col("destination").isin("SEA", "SFO", "JFK", "DEN", "ORD", "LAX", "ATL")
      )
      .groupBy("origin", "destination")
      .agg(sum("delay").as("TotalDelays"))

    departureDelaysWindow.createOrReplaceTempView("departureDelaysWindow")

    println("\n--- departureDelaysWindow (preview) ---")
    spark.sql("SELECT * FROM departureDelaysWindow").show(false)

    // SQL con dense_rank() OVER (PARTITION BY origin ORDER BY TotalDelays DESC)
    println("\n--- Top 3 by origin (SQL dense_rank) ---")
    spark.sql("""
      SELECT origin, destination, TotalDelays, rank
      FROM (
        SELECT origin, destination, TotalDelays,
               dense_rank() OVER (PARTITION BY origin ORDER BY TotalDelays DESC) AS rank
        FROM departureDelaysWindow
      ) t
      WHERE rank <= 3
      ORDER BY origin, rank
    """).show(false)

    // Alternativa DataFrame API (misma lógica)
    println("\n--- Top 3 by origin (DataFrame API) ---")
    val w = Window.partitionBy(col("origin")).orderBy(col("TotalDelays").desc)
    departureDelaysWindow
      .withColumn("rank", dense_rank().over(w))
      .filter(col("rank") <= 3)
      .orderBy(col("origin"), col("rank"))
      .show(false)
  }

  // -----------------------------
  // 5) MODIFICATIONS
  // -----------------------------
  private def modificationsExample(foo: DataFrame): Unit = {
    println("\n==============================")
    println("5) MODIFICATIONS (withColumn, drop, rename)")
    println("==============================")

    println("\n--- foo original ---")
    foo.show(false)

    // Adding new columns: status
    val foo2 = foo.withColumn(
      "status",
      expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
    )

    println("\n--- foo2 (added status) ---")
    foo2.show(false)

    // Dropping columns: delay
    val foo3 = foo2.drop("delay")

    println("\n--- foo3 (drop delay) ---")
    foo3.show(false)

    // Renaming columns: status -> flight_status
    val foo4 = foo3.withColumnRenamed("status", "flight_status")

    println("\n--- foo4 (rename status -> flight_status) ---")
    foo4.show(false)
  }

  // -----------------------------
  // 6) PIVOT
  // -----------------------------
  private def pivotExample(spark: SparkSession): Unit = {
    println("\n==============================")
    println("6) PIVOT")
    println("==============================")

    // (A) SQL pivot
    // Nota: Spark SQL soporta PIVOT, pero si tu build/catálogo fallase por el dialecto,
    // usamos la alternativa DataFrame (B) que funciona siempre.
    println("\n--- Pivot (SQL) ---")
    try {
      spark.sql("""
        SELECT * FROM (
          SELECT destination,
                 CAST(SUBSTRING(date, 0, 2) AS INT) AS month,
                 delay
          FROM departureDelays
          WHERE origin = 'SEA'
        )
        PIVOT (
          CAST(AVG(delay) AS DECIMAL(4, 2)) AS AvgDelay,
          MAX(delay) AS MaxDelay
          FOR month IN (1 JAN, 2 FEB)
        )
        ORDER BY destination
      """).show(20, truncate = false)
    } catch {
      case e: Exception =>
        println(s"SQL PIVOT failed in this environment: ${e.getMessage}")
        println("Falling back to DataFrame pivot (recommended for local execution).")
    }

    // (B) Alternativa DataFrame pivot (robusta en local)
    println("\n--- Pivot (DataFrame API fallback) ---")
    val base = spark.sql("""
      SELECT destination,
             CAST(SUBSTRING(date, 0, 2) AS INT) AS month,
             delay
      FROM departureDelays
      WHERE origin = 'SEA'
    """)

    val pivoted = base
      .groupBy(col("destination"))
      .pivot("month", Seq(1, 2))
      .agg(
        avg(col("delay")).as("AvgDelay"),
        max(col("delay")).as("MaxDelay")
      )

    // Spark crea columnas tipo: `1_AvgDelay`, `1_MaxDelay`, `2_AvgDelay`, ...
    val renamed = pivoted
      .withColumnRenamed("1_AvgDelay", "JAN_AvgDelay")
      .withColumnRenamed("1_MaxDelay", "JAN_MaxDelay")
      .withColumnRenamed("2_AvgDelay", "FEB_AvgDelay")
      .withColumnRenamed("2_MaxDelay", "FEB_MaxDelay")
      .orderBy(col("destination"))

    renamed.show(20, truncate = false)
  }
}

