package exercises.chapter9

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import io.delta.tables.DeltaTable

object Deltalake {

  // Ajusta estos paths si tu estructura difiere.
  private val sourceParquetPath = "data/chapter9/loan-risks.snappy.parquet"
  private val deltaPath         = "data/chapter9/loans_delta"
  private val streamInputPath   = "data/chapter9/new_loans_stream"
  private val checkpointDir     = "data/chapter9/checkpoints/new_loans_stream"

  def run(spark: SparkSession): Unit = {
    println("=== Chapter 9 - Delta Lake (local paths) ===")
    println(s"Parquet source:  $sourceParquetPath")
    println(s"Delta table:     $deltaPath")
    println(s"Stream input:    $streamInputPath")
    println(s"Checkpoint:      $checkpointDir")

    // 1) Load Parquet -> Delta + View
    loadParquetIntoDelta(spark)

    // 2) Explore
    showBasicQueries(spark)

    // DeltaTable handle
    val deltaTable = DeltaTable.forPath(spark, deltaPath)

    // 3) Enforcing Schema: write mismatch (expected fail)
    val loanUpdatesWithNewColumn = buildLoanUpdatesWithClosedColumn(spark)
    enforceSchemaShouldFail(loanUpdatesWithNewColumn)

    // 4) Evolve schema with mergeSchema
    evolveSchemaWithMergeSchema(loanUpdatesWithNewColumn)

    // 5) UPDATE example (OR -> WA)
    updateFixErrors(deltaTable)

    // 6) DELETE example (funded_amnt >= paid_amnt)
    deleteFullyPaid(deltaTable)

    // 7) MERGE example (upsert)
    // Nota: tras mergeSchema, la tabla ya tiene "closed". Creamos source con esa columna.
    mergeUpsertExample(deltaTable, loanUpdatesWithNewColumn)

    // 8) History (last 3)
    showHistory(deltaTable)

    // 9) Time travel (versionAsOf demo)
    timeTravelDemo(spark)

    // 10) (Opcional) Streaming file-source -> Delta append
    // Actívalo solo cuando quieras probar streaming.
    // startFileStreamIntoDelta(spark, awaitMs = 60000) // corre 60s y para
    // startFileStreamIntoDelta(spark, awaitMs = -1)    // corre indefinidamente

    println("=== Chapter 9 - Delta Lake DONE (batch parts) ===")
  }

  private def loadParquetIntoDelta(spark: SparkSession): Unit = {
    // Crea/reescribe la tabla Delta desde el Parquet. Para ejercicios suele ser conveniente overwrite.
    // Si quieres mantener estado entre ejecuciones, cambia a "append" y gestiona versiones.
    spark.read
      .format("parquet")
      .load(sourceParquetPath)
      .write
      .format("delta")
      .mode("overwrite")
      .save(deltaPath)

    spark.read
      .format("delta")
      .load(deltaPath)
      .createOrReplaceTempView("loans_delta")

    println("Loaded Parquet into Delta and created temp view loans_delta.")
  }

  private def showBasicQueries(spark: SparkSession): Unit = {
    println("== loans_delta count ==")
    spark.sql("SELECT count(*) AS cnt FROM loans_delta").show(false)

    println("== loans_delta sample (5) ==")
    spark.sql("SELECT * FROM loans_delta LIMIT 5").show(false)
  }

  private def buildLoanUpdatesWithClosedColumn(spark: SparkSession): DataFrame = {
    import spark.implicits._

    // OJO: funded_amnt en el libro es Int, paid_amnt Double.
    Seq(
      (1111111L, 1000, 1000.0, "TX", false),
      (2222222L, 2000,    0.0, "CA", true)
    ).toDF("loan_id", "funded_amnt", "paid_amnt", "addr_state", "closed")
  }

  private def enforceSchemaShouldFail(loanUpdates: DataFrame): Unit = {
    println("== Enforcing schema test: attempting append with extra column 'closed' (expected to fail if table has no 'closed') ==")

    try {
      loanUpdates.write
        .format("delta")
        .mode("append")
        .save(deltaPath)

      println("[WARN] The append did NOT fail. This likely means the Delta table schema already includes 'closed' from a previous run.")
    } catch {
      case e: Exception =>
        println("[OK] Write failed as expected due to schema mismatch.")
        println(s"Error (truncated): ${truncate(e.getMessage, 400)}")
    }
  }

  private def evolveSchemaWithMergeSchema(loanUpdates: DataFrame): Unit = {
    println("== Evolving schema with mergeSchema=true (will add 'closed' column if missing) ==")

    loanUpdates.write
      .format("delta")
      .mode("append")
      .option("mergeSchema", "true")
      .save(deltaPath)

    println("Schema evolution write completed (mergeSchema).")
  }

  private def updateFixErrors(deltaTable: DeltaTable): Unit = {
    println("== UPDATE example: addr_state OR -> WA ==")

    deltaTable.update(
      col("addr_state") === lit("OR"),
      Map("addr_state" -> lit("WA"))
    )

    println("Update completed.")
  }

  private def deleteFullyPaid(deltaTable: DeltaTable): Unit = {
    println("== DELETE example: delete rows funded_amnt >= paid_amnt ==")

    // Si hay muchas filas que cumplan esto, el dataset puede cambiar bastante.
    // Es un ejercicio de API, no tanto de negocio.
    deltaTable.delete("funded_amnt >= paid_amnt")

    println("Delete completed.")
  }

  private def mergeUpsertExample(deltaTable: DeltaTable, loanUpdates: DataFrame): Unit = {
    println("== MERGE (upsert) example on loan_id ==")

    deltaTable
      .alias("t")
      .merge(
        loanUpdates.alias("s"),
        "t.loan_id = s.loan_id"
      )
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()

    println("Merge completed.")
  }

  private def showHistory(deltaTable: DeltaTable): Unit = {
    println("== Delta history (last 3) ==")
    deltaTable
      .history(3)
      .select("version", "timestamp", "operation", "operationParameters")
      .show(false)
  }

  private def timeTravelDemo(spark: SparkSession): Unit = {
    println("== Time travel demo ==")

    // Leemos el histórico para pillar una versión anterior si existe.
    val hist = DeltaTable.forPath(spark, deltaPath).history()
    val versions = hist.select(col("version").cast("long")).orderBy(col("version").desc).limit(2).collect().map(_.getLong(0))

    if (versions.length >= 2) {
      val olderVersion = versions.last
      println(s"Reading versionAsOf = $olderVersion")

      spark.read
        .format("delta")
        .option("versionAsOf", olderVersion.toString)
        .load(deltaPath)
        .limit(5)
        .show(false)
    } else {
      println("[INFO] Not enough versions to demonstrate versionAsOf. Run the script a few times to create multiple commits.")
    }
  }

  /**
   * Structured Streaming desde carpeta de ficheros Parquet:
   * - lee nuevos Parquet que vayan cayendo en streamInputPath
   * - los añade al Delta en deltaPath
   *
   * awaitMs:
   *   -1 => indefinido
   *   >0 => milisegundos
   */
  def startFileStreamIntoDelta(spark: SparkSession, awaitMs: Long): Unit = {
    println("== Starting file stream -> Delta ==")
    println(s"Watching folder: $streamInputPath")
    println(s"Checkpoint dir:  $checkpointDir")
    println(s"Target Delta:    $deltaPath")

    // Importante: para file streaming conviene fijar schema explícito.
    // Aquí lo derivamos de la tabla Delta (ya es estable).
    val tableSchema = spark.read.format("delta").load(deltaPath).schema

    val newLoanStreamDF =
      spark.readStream
        .format("parquet")
        .schema(tableSchema)
        .load(streamInputPath)

    val query =
      newLoanStreamDF.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpointDir)
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .start(deltaPath)

    if (awaitMs < 0) {
      query.awaitTermination()
    } else {
      query.awaitTermination(awaitMs)
      query.stop()
    }

    println("== Streaming stopped ==")
  }

  private def truncate(s: String, max: Int): String = {
    if (s == null) "" else if (s.length <= max) s else s.take(max) + "..."
  }
}
