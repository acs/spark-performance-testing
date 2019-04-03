package personal.acs.spark

import scala.util.Random

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.{lit}

/**
 * @author Alvaro del Castillo <alvaro.delcastillo@gmail.com>
 */
object PerfSpark {

  val logger = Logger.getLogger(this.getClass.getName)

  val NROWS = 1000*1000*1


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

  def build_df_range(spark:SparkSession, nrows:Int): DataFrame = {
    spark.range(0, nrows).toDF("id")
  }

  def build_df_range_columns(spark:SparkSession, nrows:Int): DataFrame = {
    build_df_range(spark, nrows).toDF("id")
                                .withColumn("a", lit("ColumnA"))
                                .withColumn("b", lit("ColumnB"))
                                .withColumn("c", lit("ColumnC"))
                                .withColumn("d", lit("ColumnD"))
                                .withColumn("e", lit("ColumnE"))
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
    // val df = build_df_random(spark, NROWS)
    val df = build_df_range(spark, NROWS)

    df.join(df, "id")
  }

  def auto_join_cache(spark: SparkSession): DataFrame = {
    // val df = build_df(spark)
    // val df = build_df_random(spark, 100)
    val df = build_df_range(spark, NROWS)


    df.cache()
    df.join(df, "id")
  }


  def basic_join(spark: SparkSession): DataFrame = {
    // val df = build_df(spark)
    // val df1 = build_df(spark)
    val df = build_df_random(spark, NROWS)
    val df1 = build_df_random(spark, NROWS)

    df.join(df1, "id")
  }

  def basic_join_left_anti(spark: SparkSession): DataFrame = {
    val df = build_df(spark)
    val df1 = build_df(spark)
    df.join(df1, Seq("id"), "left_anti")
  }


  def multi_join_from_range(spark: SparkSession): DataFrame = {
    // If the nrows for the dataframe is the same, catalyst detects that all the
    // dataframes are the same and optimize the process
    // In this sample we are forcing different dataframes
    val df = build_df_range(spark, NROWS)
    val df1 = build_df_range(spark, NROWS)
    val df2 = build_df_range(spark, NROWS)
    val df3 = build_df_range(spark, NROWS)
    val df4 = build_df_range(spark, NROWS)

    df.join(df1, "id").join(df2, "id").join(df3, "id").join(df4, "id")
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

  def union(spark:SparkSession): DataFrame = {
    val df = build_df_range(spark, NROWS)
    val df1 = build_df_range(spark, NROWS)
    val df2 = build_df_range(spark, NROWS)
    val df3 = build_df_range(spark, NROWS)
    val df4 = build_df_range(spark, NROWS)
    df.union(df1).union(df2).union(df3).union(df4)
  }

  def checkpoint(spark:SparkSession): DataFrame = {
    // Sometimes breaking up a long lineage graph
    // for its own sake can help a job succeed since it means each of the tasks will be smaller.
    // Breaking lineage between narrow transformations is only desirable in the most extreme cases
    // If a job is failing with GC or out-of-memory errors,
    // checkpointing or persisting off_heap may allow the job to complete, particularly if
    // the cluster is noisy
    // The best way to tell if you need to reuse your RDDs is to run a job
    // Set the checkpoint directory
    spark.sparkContext.setCheckpointDir("/tmp/checkpoint")

    val df = build_df(spark)
    val joinNames = df.columns.map("auto_" +  _)
    val renamedDF = df.toDF(joinNames: _*)
    val df_join = df.join(renamedDF, renamedDF("auto_id") === df("id"))
    // https://github.com/JerryLead/SparkInternals/blob/master/markdown/english/6-CacheAndCheckpoint.md#checkpoint
    // http://www.lewisgavin.co.uk/Spark-Performance
    // https://www.waitingforcode.com/apache-spark/checkpointing-in-spark/read
    // Checkpoint persistence could be the HDFS so it is fault tolerant, with high capacity ....
    // TODO: Policy for cleaning the checkpointed df? Just remove the directory using HDFS API?
    val df_checkpointed = df_join.checkpoint()
    // Let's show the logical plans to show that the checkpoint is just a direct RDD
    println("Logical plan for the same dataframe but the second version checkpointed")
    println(df_join.queryExecution.logical)
    println(df_checkpointed.queryExecution.logical)
    df_checkpointed
  }

  def duplicates(spark:SparkSession): DataFrame = {
    val df = build_df_range_columns(spark, NROWS)
    val df1 = build_df_range_columns(spark, NROWS)

    val dfUnionDuplicates = df.union(df1)
    dfUnionDuplicates.count()

    val dfUnique = dfUnionDuplicates.dropDuplicates("id")

    // distinct is just an alias for dropDuplicates so no need to test it
    // val distinct_count = dfUnionDuplicates.distinct().count()

    dfUnique
  }


  def persist(spark:SparkSession): Unit = {
    val df = build_df(spark)
    val df_join = df.join(df, "id")

    // The persistence is done in a local directory
    // https://stackoverflow.com/questions/48430366/where-is-my-sparkdf-persistdisk-only-data-stored
    val df_persist_disk = df_join.persist(StorageLevel.DISK_ONLY)

    // One way to get a feel for the size of the RDD is to write a program
    //  that caches the RDD. Then, launch the job and while it is running,
    // look at the “Storage” page of the web UI.
  }



  def main(args : Array[String]) {
    val spark = SparkSession.builder()
      .appName("Spark performance testing")
      .master("local[*]")
      .getOrCreate()

    // basic_rdd(spark)
    // print(s"Total join rows ${auto_join(spark).count()}")
    // auto_join_cache(spark).show(1000)
    // auto_join_cache(spark).count()
    // basic_join(spark).count()
    // basic_join_left_anti(spark).count()
    // multi_join_from_range(spark).count()
    // union(spark).count()
    checkpoint(spark)
    // duplicates(spark).count()

    Thread.sleep(1000*1000)

  }

}
