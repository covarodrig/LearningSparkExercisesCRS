package exercises.chapter6

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import scala.util.Random

object Datasets {

  // Case class base del capítulo
  case class Usage(uid: Int, uname: String, usage: Int)

  // Case class extendida con coste
  case class UsageCost(uid: Int, uname: String, usage: Int, cost: Double)

  def run(spark: SparkSession): Unit = {
    implicit val implicitSpark: SparkSession = spark
    import spark.implicits._

    // =========================================================
    // 1) Crear datos de ejemplo (1000 instancias) -> Dataset[Usage]
    // =========================================================
    val r = new Random(42)

    val data: Seq[Usage] =
      (0 to 1000).map { i =>
        Usage(
          uid = i,
          uname = "user-" + r.alphanumeric.take(5).mkString(""),
          usage = r.nextInt(1000)
        )
      }

    val dsUsage: Dataset[Usage] = spark.createDataset(data)

    println("\n=== dsUsage (sample) ===")
    dsUsage.show(10, truncate = false)
    dsUsage.printSchema()

    // =========================================================
    // 2) filter() con lambda
    // =========================================================
    println("\n=== filter() con lambda: usage > 900 ===")
    dsUsage
      .filter(u => u.usage > 900)
      .orderBy(desc("usage"))
      .show(5, truncate = false)

    // =========================================================
    // 3) filter() con función nombrada (corrige tu error)
    // =========================================================
    def filterWithUsage(u: Usage): Boolean = u.usage > 900

    println("\n=== filter() con función nombrada: usage > 900 ===")
    dsUsage
      .filter(filterWithUsage _) // clave: método -> función
      .orderBy(desc("usage"))
      .show(5, truncate = false)

    // =========================================================
    // 4) map() devolviendo un valor (Dataset[Double])
    // =========================================================
    println("\n=== map() a Double: coste por usuario (sin uid/uname) ===")
    dsUsage
      .map(u => if (u.usage > 750) u.usage * 0.15 else u.usage * 0.50)
      .show(5, truncate = false)

    def computeCostUsage(usage: Int): Double =
      if (usage > 750) usage * 0.15 else usage * 0.50

    println("\n=== map() a Double usando función computeCostUsage ===")
    dsUsage
      .map(u => computeCostUsage(u.usage))
      .show(5, truncate = false)

    // =========================================================
    // 5) map() a un nuevo tipo fuerte (Dataset[UsageCost])
    // =========================================================
    def computeUserCostUsage(u: Usage): UsageCost = {
      val cost = computeCostUsage(u.usage)
      UsageCost(u.uid, u.uname, u.usage, cost)
    }

    val dsUsageCost: Dataset[UsageCost] =
      dsUsage.map(u => computeUserCostUsage(u))

    println("\n=== Dataset[UsageCost]: incluye uid/uname/usage + cost ===")
    dsUsageCost.show(10, truncate = false)
    dsUsageCost.printSchema()

    println("\n=== Top 10 por cost DESC ===")
    dsUsageCost
      .orderBy(desc("cost"))
      .show(10, truncate = false)
  }
}

