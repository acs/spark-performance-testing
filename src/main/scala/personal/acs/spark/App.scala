package personal.acs.spark

import scala.util.Random

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType, IntegerType, StringType}

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

  def build_df_with_schema(spark:SparkSession): DataFrame = {
    val someData = Seq(
      Row(1, "bat"),
      Row(2, "mouse"),
      Row(3, "horse")
    )

    val someSchema = List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true)
    )

    spark.createDataFrame(
      spark.sparkContext.parallelize(someData),
      StructType(someSchema)
    )
  }

  def build_df(spark:SparkSession): DataFrame = {
    import spark.implicits._

    val COLS_TEST_NAMES = List("id", "country", "entity")

    val scalaList = Seq(("id1", "cou1", "en1"), ("id2", "cou2", "en2"))
    scalaList.toDF(COLS_TEST_NAMES: _*)
  }

  def build_df_range(spark:SparkSession): Dataset[java.lang.Long] = {
    spark.range(0, 10)
  }


  def build_df_random(spark:SparkSession, nrows:Integer): DataFrame = {
    import spark.implicits._

    // In this implementation the full generation is done in the driver
    // Seq.fill(nrows)(nrows).flatMap(row => Seq.fill(row)(Random.nextInt)).toDF("id")
    // The seed in created in the driver
    val seedRdd = spark.sparkContext.parallelize(Seq.fill(nrows)(nrows))
    // The complete df is generated in the executors
    seedRdd.flatMap(records => Seq.fill(records)(Random.nextInt)).toDF("id")
  }


  def auto_join(spark: SparkSession): DataFrame = {
    // val df = build_df(spark)
    val df = build_df_random(spark, 1000)
    df.join(df, "id")
  }

  def auto_join_cache(spark: SparkSession): DataFrame = {
    // val df = build_df(spark)
    val df = build_df_random(spark, 1000)

    df.cache()
    df.join(df, "id")
  }


  def basic_join(spark: SparkSession): DataFrame = {
    // val df = build_df(spark)
    // val df1 = build_df(spark)
    val df = build_df_random(spark, 1000)
    val df1 = build_df_random(spark, 1000)

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

    // basic_rdd(spark)
    auto_join(spark).count()
    auto_join_cache(spark).count()
    basic_join(spark).count()
    // basic_join_left_anti(spark).count()

    Thread.sleep(1000*1000)

  }

}
