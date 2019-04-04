package personal.acs.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.{col, first, max}

object PerfSparkSQL {

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

  def main(args: Array[String]) {
    println("Testing the performance in SparkSQL")

    val spark = SparkSession.builder()
      .appName("Spark performance testing")
      .master("local[*]")
      .getOrCreate()

    // The approach is to play with the transforms and actions API
    // for Dataframes (and at some point with Datasets)
    // https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset

    selectDuplicates(spark)
  }
}