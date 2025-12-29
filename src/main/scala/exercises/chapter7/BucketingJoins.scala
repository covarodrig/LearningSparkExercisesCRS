package exercises.chapter7

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object BucketingJoins {

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    // =========
    // Windows workarounds (clave para evitar NativeIO$Windows.access0)
    // =========
    // 1) Evitar librerías nativas de Hadoop (JNI NativeIO) en Windows
    spark.sparkContext.hadoopConfiguration.setBoolean("hadoop.native.lib", false)

    // 2) Committer menos problemático en Windows
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

    // 3) Commit protocol para Spark SQL (ayuda en algunos casos)
    spark.conf.set(
      "spark.sql.sources.commitProtocolClass",
      "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol"
    )

    // =========
    // Datos de ejemplo
    // =========
    val usersDF = Seq(
      (1, "Alice", "CO"),
      (2, "Bob", "NY"),
      (3, "Charlie", "TX"),
      (4, "Diana", "CA"),
      (22, "Eve", "CO")
    ).toDF("uid", "name", "user_state")

    val ordersDF = Seq(
      (144179, 144179, 22, 288358.0, "TX", "SKU-4"),
      (145352, 145352, 22, 290704.0, "NY", "SKU-0"),
      (168648, 168648, 22, 337296.0, "TX", "SKU-2"),
      (173682, 173682, 22, 347364.0, "NY", "SKU-2"),
      (397577, 397577, 22, 795154.0, "CA", "SKU-3"),
      (403974, 403974, 22, 807948.0, "CO", "SKU-2"),
      (405438, 405438, 22, 810876.0, "NY", "SKU-1"),
      (417886, 417886, 22, 835772.0, "CA", "SKU-3"),
      (420809, 420809, 22, 841618.0, "NY", "SKU-4"),
      (500001, 2, 2, 120.0, "NY", "SKU-9"),
      (500002, 1, 3, 75.5, "TX", "SKU-7"),
      (500003, 5, 4, 510.0, "CA", "SKU-1")
    ).toDF("transaction_id", "quantity", "users_id", "amount", "state", "items")

    println("== usersDF sample ==")
    usersDF.show(false)
    println("== ordersDF sample ==")
    ordersDF.show(false)

    // =========
    // Opción A: NO usar saveAsTable/bucketing real (evitas metastore/warehouse),
    // pero mantienes un escenario controlado para join + explain
    // =========
    val baseOut = "C:/spark-output/bucketing_joins"
    val usersPath  = s"$baseOut/users_parquet"
    val ordersPath = s"$baseOut/orders_parquet"

    // Escritura simple (sin bucketBy) para validar que el IO ya no revienta
    usersDF
      .orderBy($"uid".asc)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(usersPath)

    ordersDF
      .orderBy($"users_id".asc)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(ordersPath)

    val usersParquetDF  = spark.read.parquet(usersPath)
    val ordersParquetDF = spark.read.parquet(ordersPath)

    // Cache opcional
    usersParquetDF.cache()
    ordersParquetDF.cache()

    val joined =
      ordersParquetDF.join(usersParquetDF, $"users_id" === $"uid", "inner")

    println("== joined ==")
    joined.show(50, false)

    println("== Physical plan (explain(true)) ==")
    joined.explain(true)

    println("Abre la Spark UI mientras corre: http://localhost:4040 (o 4041, 4042...)")
    println("Pulsa ENTER para terminar el ejercicio...")
    scala.io.StdIn.readLine()
  }
}
