package personal.acs.spark

import scala.util.control.NonFatal
import java.io.{File, FileOutputStream}

import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.{col, first, max}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row
import personal.acs.spark.PerfSpark.{build_df_random}

object PerfSparkJoins {


  case class Sample(id: Int, col1: String, col2: String)


  /**
    * Show the basic join in SparkSQL and their performance
    * https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset
    *
    * @param spark
    */
  def crossJoin(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = Seq((1,"a0","b0"), (2, "a1", "b1")).toDF("id", "col1", "col2")
    val df1 = Seq((1001,"c0","d0"), (1002, "c1", "d1")).toDF("id", "col1", "col2")

    val emptyDf = spark.emptyDataset[Sample].toDF()

    // Force the cartesian join disabled by default
    spark.conf.set("spark.sql.crossJoin.enabled", true)

    df.join(df1).show()

    // And now let's catch the cartesian product
    spark.conf.set("spark.sql.crossJoin.enabled", false)

    try {
      val joinDf = df.join(df1)
      joinDf.show()
    } catch {
      case ex: org.apache.spark.sql.AnalysisException => {
        println("Problems with the join", ex.message)
      }
      case NonFatal(ex) => println("Exception ignored", ex.getMessage)
    }
  }

  def basicJoin(spark: SparkSession): Unit = {
    import spark.implicits._
    val df =
      Seq((1,"a0","b0"), (2, "a1", "b1"))
      .toDF("id", "col1", "col2")
    val df1 =
      Seq((1001,"c0","d0"), (1002, "c1", "d1"), (1, "df1a", "df1b"))
      .toDF("id", "col1", "col2")

    df.as("b")
      .join(df1.alias("a"), col("a.id") === col("b.id"))
      .select("b.col1")
      .show()
    df.as("b")
      .join(df1.alias("a"), col("a.id") === col("b.id"))
      .select(col("a.id"), col("b.col2"))
      .select("a.id", "b.col2")
      .show()
    df.join(df1, Seq("id")).show()
    df.join(df1, "id").show()
  }

  /**
    * This method is used to pressure the memory usage of Spark
    *
    * @param spark: already created Spark session
    */
  def bigJoin(spark: SparkSession): Unit = {
    // -Xmx512m
    val cache = true  // use cache to add extra pressure to Spark memory
    // >= 6: ERROR Executor: Exception in task 4.0 in stage 4.0 (TID 22)
    //        java.lang.OutOfMemoryError: GC overhead limit exceeded
    val pressure = 6
    val NROWS = 1000 * pressure

    val df = build_df_random(spark, NROWS)
    val df1 = build_df_random(spark, NROWS)

    spark.sparkContext.setCheckpointDir("/tmp/rdd")

    // df.checkpoint()
    // df1.checkpoint()

    if (cache) {
      df.cache()
      df1.cache()
    }
    println(s"dfs to join: ${df.count()} ${df1.count()}")
    println(s"Number of rows in join: ${df.join(df1, "id").count()}")
    // df.unpersist()
    // df1.unpersist()
  }



  def main(args: Array[String]) {
    val initime = System.nanoTime

    println("Testing the performance in SparkSQL JOINS")

    val spark = SparkSession.builder()
      .appName("Spark performance testing")
      .master("local[*]")
      .getOrCreate()

    // Joins performance is critical in Spark applications
    // Share partitioning schema in the to be joined dataframes
    // ShuffleJoin vs BroadcastJoin
    // Join order
    // Different types of join
    // Columns used for joining the tables

    // crossJoin(spark)
    bigJoin(spark)

    println(s"Total time: ${(System.nanoTime - initime) / 1e9d}")

    // Thread.sleep(1000*1000)
  }
}