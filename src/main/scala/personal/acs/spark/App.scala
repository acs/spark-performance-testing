package personal.acs.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author Alvaro del Castillo <alvaro.delcastillo@gmail.com>
 */
object App {

  def basic_rdd(spark: SparkSession): Unit = {
    val sc = spark.sparkContext

    // Sample creation of RDD from a Scala collection
    val col = sc.parallelize(0 to 100 by 5)
    val smp = col.sample(true, 4)
    val colCount = col.count
    val smpCount = smp.count

    println("orig count = " + colCount)
    println("sampled count = " + smpCount)
  }

  def build_df(spark:SparkSession): DataFrame = {
    import spark.implicits._

    val COLS_TEST_NAMES = List("id", "country", "entity")

    val scalaList = Seq(("id1", "cou1", "en1"), ("id2", "cou2", "en2"))
    scalaList.toDF(COLS_TEST_NAMES: _*)
  }

  def basic_df(spark: SparkSession): Unit = {
    val df = build_df(spark)
    df.explain(true)
    df.show()
  }

  def basic_join(spark: SparkSession): DataFrame = {
    val df = build_df(spark)
    val df1 = build_df(spark)
    df.join(df1, "id")
  }

  def basic_join_left_anti(spark: SparkSession): DataFrame = {
    val df = build_df(spark)
    val df1 = build_df(spark)
    df.join(df1, Seq("id"), "left_anti")
  }


  def multi_join(spark: SparkSession): DataFrame = {
    val df = build_df(spark)
    val df1 = build_df(spark)
    val df2 = build_df(spark)
    val df3 = build_df(spark)
    val df4 = build_df(spark)

    df.join(df1, "id").join(df2, "id").join(df3, "id").join(df4, "id")
  }

  def schema_change(spark:SparkSession): Unit = {

  }

  def union(spark:SparkSession): Unit = {

  }

  def persist(spark:SparkSession): Unit = {

  }



  def main(args : Array[String]) {
    val spark = SparkSession.builder()
      .appName("Spark performance testing")
      .master("local[*]")
      .getOrCreate()

    basic_rdd(spark)
    basic_df(spark)
    basic_join(spark).count()
    basic_join_left_anti(spark).count()

    Thread.sleep(1000*1000)

  }

}
