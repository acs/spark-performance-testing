package personal.acs.spark

import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._


// https://www.elastic.co/guide/en/elasticsearch/hadoop/master/spark.html

object Elasticsearch {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Elasticsearch Integration")
      .master("local[*]")
      .getOrCreate()



    // Configure the Elasticsearch index in which to write the dataframe data
    spark.conf.set("es.index.auto.create", "true")

    //  Create a new dataframe and store it in ES

    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    spark.sparkContext.makeRDD(
      Seq(numbers, airports)
    ).saveToEs("spark-es/items")

    // Writing existing JSON to Elasticsearch
    // spark.sparkContext.makeRDD(Seq(json1, json2)).saveJsonToEs("spark/json-trips")

    // Reading data from Elasticsearch
    // val RDD = spark.sparkContext.esRDD("radio/artists")
    // val people = sql.esDF("spark/people")
  }

}
