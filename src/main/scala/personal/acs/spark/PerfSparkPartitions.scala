package personal.acs.spark

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{lit}

import java.io.File

/**
  * The goal of this object is to test how the different partition strategies
  * affect the Spark performance
  */
object PerfSparkPartitions {

  def write_append(spark:SparkSession): Unit = {
    val df = PerfSpark.build_df_random(spark, 10)
    val path = "/tmp/df-partitions"

    df.write.mode(SaveMode.Append).parquet(path)
    val dfn = spark.read.parquet(path)
    println(s"Total number of rows ${dfn.count()} and partitions ${dfn.rdd.partitions.size}")
    df.write.mode(SaveMode.Append).parquet(path)
    val dfn1 = spark.read.parquet(path)
    println(s"Total number of rows ${dfn1.count()} and partitions ${dfn1.rdd.partitions.size}")
    println(s"Total number of files in parquet ${(new File(path).listFiles().size/2)-1}")
  }

  def write_partitionBy(spark:SparkSession): Unit = {
    val df = PerfSpark.build_df_range_columns(spark, 100)
    val path = "/tmp/df-partitionBy"

    df.write.mode(SaveMode.Overwrite).partitionBy("a", "id").parquet(path)

    val pathid = "/tmp/df-partitionBy/a=ColumnA/id=99"

    // If a partitioned parquet file is read, the column is not included
    // a, id cols are not included in this case
    spark.read.parquet(pathid).show()

    // https://dzone.com/articles/performance-implications-of-partitioning-in-apache
    // Dynamic partition concept is pretty cool:
    // https://medium.com/a-muggles-pensieve/writing-into-dynamic-partitions-using-spark-2e2b818a007a
  }

  /**
    * Is it possible to rewrite only some partitions that are updated?
    *
    * @param spark
    */
  def rewrite(spark:SparkSession): Unit = {

  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark performance testing")
      .master("local[*]")
      .getOrCreate()

    // write_append(spark)
    write_partitionBy(spark)
  }
}


