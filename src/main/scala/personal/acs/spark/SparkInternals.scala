package personal.acs.spark

import org.apache.spark.sql.SparkSession

object SparkInternals {

  /**
    * head action is implemented using limit, but limit is a transformation
    * that is executed in head action. To travel in spark execution chain is a great sample.
    * But it is not easy to follow all the code the first time. Be patient.
    *
    * def head(n: Int): Array[T] = withAction("head", limit(n).queryExecution)(collectFromPlan)
    *
    * @param spark
    */
  def showHeadLimit(spark:SparkSession): Unit = {
    import spark.implicits._

    val df = Seq((1,"a0","b0"), (2, "a1", "b1")).toDF("id", "col1", "col2")

    df.limit(10)
    df.head(10)
  }

  def main(args: Array[String]) {
    println("Showing how Spark internals works")

    val spark = SparkSession.builder()
      .appName("Spark performance testing")
      .master("local[*]")
      .getOrCreate()

    showHeadLimit(spark)
  }
}
