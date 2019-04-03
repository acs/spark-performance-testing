package personal.acs.spark

import org.apache.spark.sql.SparkSession


object PerfSparkSQL {

  def main(args: Array[String]) {
    println("Testing the performance in SparkSQL")

    def main(args: Array[String]) {
      val spark = SparkSession.builder()
        .appName("Spark performance testing")
        .master("local[*]")
        .getOrCreate()

      PerfSpark.build_df(spark)
    }
  }
}