package personal.acs.spark

import java.io.{File, FileOutputStream}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.{col, first, max}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row

object PerfSparkSQL {

  /**
    * Show the basic transformatiosn in SparkSQL and their performance
    * https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset
    *
    * @param sparkSession
    */
  def execBasicTrans(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    // Exception for mixing different types
    // val df = Seq((1,2),(12,22),("a","b")).toDF("col1", "col2")
    val df = Seq((1,"a0","b0"), (2, "a1", "b1")).toDF("id", "col1", "col2")
    val df1 = Seq((1001,"c0","d0"), (1002, "c1", "d1")).toDF("id", "col1", "col2")

    df.union(df1).show(true)
    df.printSchema()

    df.intersect(df1).show()
    df.join(df1).show()

    df.take(10)
    df.head()
  }

  /**
    * Show the best way to modify the schema of a Dataframe to adapt it
    *
    * @param spark
    * @return
    */

  def changeSchema(spark:SparkSession): DataFrame = {
    val sc = spark.sparkContext

    //Create Schema RDD
    val schema_string = "name, id, dept"
    val schema_rdd = StructType(schema_string.split(",").map(fieldName => StructField(fieldName, StringType, true)) )

    //Create Empty DataFrame
    val empty_df = spark.sqlContext.createDataFrame(sc.emptyRDD[Row], schema_rdd)

    // TODO: complete the samples

    empty_df
  }

  /**
    * Select the row to use when mixing duplicates
    * https://quynhcodes.wordpress.com/2016/07/29/drop-duplicates-by-some-condition/
    * @param spark
    * @return
    */
  def selectDuplicates(spark: SparkSession): DataFrame = {
    val df = PerfSpark.build_df_range(spark, 10)
    val df_to_drop = df.withColumn("order", lit(0))
    val df1 = PerfSpark.build_df_range_columns(spark, 7)
    val df1_to_drop = df1.withColumn("order", lit(1))
    val df_union = df_to_drop
      .withColumn("ColumnA", lit(""))
      .withColumn("ColumnB", lit(""))
      .withColumn("ColumnC", lit(""))
      .withColumn("ColumnD", lit(""))
      .withColumn("ColumnE", lit(""))
      .union(df1_to_drop)
    df_union.show()
    df_union.dropDuplicates("id").show()

    val df_clean = df_union
      .orderBy(col("order").desc)
      .groupBy("id")
      .agg(df_union("id"), first(df_union("ColumnA")))

    val df_clean1 = df_union
      .orderBy(col("order").asc)
      .groupBy("id")
      .agg(df_union("id"), first(df_union("ColumnA")))

    df_clean.show()
    df_clean1.show()

    // Now lets do the same but using left-anti join
    val df_clean_leftanti = df.join(df1, Seq("id"), "leftanti")
    val df_clean_leftanti_1 = df1.join(df, Seq("id"), "leftanti")
    df_clean_leftanti.show()
    df_clean_leftanti_1.show()

    df_union.dropDuplicates()
  }

  def load_github_df(spark: SparkSession): DataFrame = {
    // Github data must be already downloaded using wget
    // wget http://data.githubarchive.org/2015-03-01-{0..23}.json.gz
    val data_path = "github-data"
    // when you create a DataFrame from a structured
    // dataset (in this case, JSON), Spark is able to infer a schema by making a pass over
    // the entire JSON dataset that’s being loaded
    // Just the same way Elasticsearch does mapping infer
    val homeDir = System.getenv("HOME")
    val ghdate = "2015-03-01-0.json"
    val ghLog = spark.read.json(homeDir + "/github-data/" + ghdate)
    ghLog.printSchema()
    println(s"Events in ${ghdate}: ${ghLog.count()}")
    ghLog
  }

  def main(args: Array[String]) {
    println("Testing the performance in SparkSQL")

    val spark = SparkSession.builder()
      .appName("Spark performance testing")
      .master("local[*]")
      .getOrCreate()

    // The approach is to play with the transforms and actions API
    // for Dataframes (and at some point with Datasets)
    // https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset

    // selectDuplicates(spark)
    // changeSchema(spark).show()
    // execBasicTrans(spark)
    load_github_df(spark)
  }
}