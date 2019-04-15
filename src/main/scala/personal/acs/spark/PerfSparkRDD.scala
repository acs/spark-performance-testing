package personal.acs.spark

import org.apache.spark.sql.SparkSession

object PerfSparkRDD {

  def aggregations(spark:SparkSession): Unit = {
    val words = Array("one", "two", "two", "three", "three", "three")
    val wordPairsRDD = spark.sparkContext.parallelize(words).map(word => (word, 1))

    val wordCountsWithReduce = wordPairsRDD
      .reduceByKey((a, b) => a + b)
      .collect()

    // The performance in this operation is pretty bad
    // https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html
    val wordCountsWithGroup = wordPairsRDD
      .groupByKey()
      .map(t => (t._1, t._2.sum))
      .collect()

    val res = 0
  }

  def join(spark:SparkSession): Unit = {
    // let's play with zip tha join two dataframes
  }

  def main(args: Array[String]) {
    println("Testing the performance in SparkRDD")

    val spark = SparkSession.builder()
      .appName("Spark performance testing")
      .master("local[*]")
      .getOrCreate()

    aggregations(spark)

  }
}
