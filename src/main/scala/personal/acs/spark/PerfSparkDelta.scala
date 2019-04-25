package personal.acs.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

object PerfSparkDelta {

  def main(args: Array[String]) {
    println("Testing the performance in SparkRDD using Delta")

    val spark = SparkSession.builder()
      .appName("Spark performance testing")
      .master("local[*]")
      .getOrCreate()


    spark.range(0, 10).write.format("delta").mode(SaveMode.Overwrite)save("/tmp/delta-table")
    spark.range(11, 20).write.format("delta").mode(SaveMode.Overwrite)save("/tmp/delta-table")
    spark.range(21, 30).write.format("delta").mode(SaveMode.Overwrite)save("/tmp/delta-table")

    spark.read.format("delta").load("/tmp/delta-table").show()
    spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta-table").show()
    spark.read.format("delta").option("versionAsOf", 1).load("/tmp/delta-table").show()
    spark.read.format("delta").option("versionAsOf", 2).load("/tmp/delta-table").show()
  }
}
