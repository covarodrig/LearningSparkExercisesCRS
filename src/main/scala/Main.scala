import utils.Spark
import exercises.chapter2.Chapter2Runner
import exercises.chapter3.Chapter3Runner

object Main {
  def main(args: Array[String]): Unit = {
    val spark = Spark.getSparkSession("LearningSparkExercisesCRS")
    try {
      // Chapter2Runner.run(spark)
      Chapter3Runner.run(spark)
      // Chapter4Runner.run(spark)
    } finally {
      spark.stop()
    }
  }
}



