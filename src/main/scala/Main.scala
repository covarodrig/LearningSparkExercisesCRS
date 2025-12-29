import utils.Spark
import exercises.chapter2.Chapter2Runner
import exercises.chapter3.Chapter3Runner
import exercises.chapter4.ExerciseFlights
import exercises.chapter5.RelationalOps
import exercises.chapter6.Datasets
import exercises.chapter7.BucketingJoins
import exercises.chapter9.Deltalake

object Main {
  def main(args: Array[String]): Unit = {
    val spark = Spark.getSparkSession("LearningSparkExercisesCRS")
    try {
      // Chapter2Runner.run(spark)
      // Chapter3Runner.run(spark)
      // ExerciseFlights.run(spark)
      // RelationalOps.run(spark)
      // Datasets.run(spark)
      // BucketingJoins.run(spark)
      Deltalake.run(spark)
    } finally {
      spark.stop()
    }
  }
}




