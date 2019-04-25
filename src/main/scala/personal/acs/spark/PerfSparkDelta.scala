package personal.acs.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

object PerfSparkDelta {

  def main(args: Array[String]) {
    println("Testing the performance in SparkRDD using Delta")

    val spark = SparkSession.builder()
      .appName("Spark performance testing")
      .master("local[*]")
      .getOrCreate()

    val data = spark.range(0, 50000)
    data.write.format("delta").mode(SaveMode.Overwrite)save("/tmp/delta-table")

  }
}
