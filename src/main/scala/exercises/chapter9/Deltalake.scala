package exercises.chapter9

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import io.delta.tables.DeltaTable

object Deltalake {

  private val sourceParquetPath = "data/chapter9/loan-risks.snappy.parquet"
  private val deltaPath         = "data/chapter9/loans_delta"
  private val streamInputPath   = "data/chapter9/new_loans_stream"
  private val checkpointDir     = "data/chapter9/checkpoints/new_loans_stream"

  def run(spark: SparkSession): Unit = {
    println("=== Chapter 9 - Delta Lake ===")

    loadParquetIntoDelta(spark)
    showBasicQueries(spark)

    val deltaTable = DeltaTable.forPath(spark, deltaPath)

    val loanUpdatesWithNewColumn = buildLoanUpdatesWithClosedColumn(spark)
    enforceSchemaShouldFail(loanUpdatesWithNewColumn)
    evolveSchemaWithMergeSchema(loanUpdatesWithNewColumn)

    updateFixErrors(deltaTable)
    deleteFullyPaid(deltaTable)

    mergeUpsertExample(spark, loanUpdatesWithNewColumn)

    showHistory(deltaTable)
    timeTravelDemo(spark)

    println("=== Chapter 9 DONE ===")
  }

  private def loadParquetIntoDelta(spark: SparkSession): Unit = {
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
  }

  private def showBasicQueries(spark: SparkSession): Unit = {
    spark.sql("SELECT count(*) FROM loans_delta").show(false)
    spark.sql("SELECT * FROM loans_delta LIMIT 5").show(false)
  }

  private def buildLoanUpdatesWithClosedColumn(spark: SparkSession): DataFrame = {
    import spark.implicits._

    Seq(
      (1111111L, 1000, 1000.0, "TX", false),
      (2222222L, 2000,    0.0, "CA", true)
    ).toDF("loan_id", "funded_amnt", "paid_amnt", "addr_state", "closed")
  }

  private def enforceSchemaShouldFail(loanUpdates: DataFrame): Unit = {
    try {
      loanUpdates.write
        .format("delta")
        .mode("append")
        .save(deltaPath)
    } catch {
      case _: Exception => ()
    }
  }

  private def evolveSchemaWithMergeSchema(loanUpdates: DataFrame): Unit = {
    loanUpdates.write
      .format("delta")
      .mode("append")
      .option("mergeSchema", "true")
      .save(deltaPath)
  }

  private def updateFixErrors(deltaTable: DeltaTable): Unit = {
    deltaTable.update(
      col("addr_state") === lit("OR"),
      Map("addr_state" -> lit("WA"))
    )
  }

  private def deleteFullyPaid(deltaTable: DeltaTable): Unit = {
    deltaTable.delete("funded_amnt >= paid_amnt")
  }

  private def mergeUpsertExample(spark: SparkSession, loanUpdates: DataFrame): Unit = {
    val freshDeltaTable = DeltaTable.forPath(spark, deltaPath)

    freshDeltaTable
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
  }

  private def showHistory(deltaTable: DeltaTable): Unit = {
    deltaTable
      .history(3)
      .select("version", "timestamp", "operation")
      .show(false)
  }

  private def timeTravelDemo(spark: SparkSession): Unit = {
    val versions = DeltaTable.forPath(spark, deltaPath)
      .history()
      .select(col("version").cast("long"))
      .orderBy(col("version").desc)
      .limit(2)
      .collect()
      .map(_.getLong(0))

    if (versions.length >= 2) {
      spark.read
        .format("delta")
        .option("versionAsOf", versions.last.toString)
        .load(deltaPath)
        .limit(5)
        .show(false)
    }
  }

  def startFileStreamIntoDelta(spark: SparkSession, awaitMs: Long): Unit = {
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

    if (awaitMs < 0) query.awaitTermination()
    else {
      query.awaitTermination(awaitMs)
      query.stop()
    }
  }
}


