package personal.acs.spark

import org.apache.spark.sql.SparkSession

/**
 * @author Alvaro del Castillo <alvaro.delcastillo@gmail.com>
 */
object App {

  def main(args : Array[String]) {
    val spark = SparkSession.builder()
      .appName("Spark performance testing")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val col = sc.parallelize(0 to 100 by 5)
    val smp = col.sample(true, 4)
    val colCount = col.count
    val smpCount = smp.count

    println("orig count = " + colCount)
    println("sampled count = " + smpCount)
  }

}
