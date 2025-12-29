package utils

import org.apache.spark.sql.SparkSession

object Spark {

  def getSparkSession(appName: String): SparkSession = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    SparkSession.builder()
      .appName(appName)
      .master("local[*]")

      .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")

      // Hadoop native IO OFF (Windows)
      .config("spark.hadoop.fs.nativeio.enabled", "false")
      .config("spark.hadoop.fs.file.impl.disable.cache", "true")

      // Delta Lake integration (recomendado)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

      // CRÍTICO en Windows: evitar HDFSLogStore sobre file:
      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.LocalLogStore")

      // para que no dependa de Hive
      .config("spark.sql.catalogImplementation", "in-memory")

      .getOrCreate()
  }
}
