package personal.acs.spark


import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.coding.Gzip
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.FileIO
import org.apache.spark.sql.SparkSession

import scala.concurrent.Future
import scala.util.{Failure, Success}

object AkkaHTTPGet {

  val json_url = "https://data.gharchive.org/2015-03-01-0.json.gz"
  val json_file = "/tmp/" + json_url.split("/")(3).replace(".gz","")

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = json_url))

    responseFuture.flatMap { response =>
      val source = response.entity.dataBytes
      val path = Paths.get(json_file)
      source.via(Gzip.decoderFlow).runWith(FileIO.toPath(path))
      // TODO: Learn Akka to avoid this sleep
      Thread.sleep(10*1000)
      // Let's play with the JSON file
      val spark = SparkSession.builder()
        .appName("Spark performance testing")
        .master("local[*]")
        .getOrCreate()
      val df = spark.read.json(json_file)
      println(s"Total events in ${json_url} ${df.count()}")

      system.terminate()
    }

//    responseFuture
//      .onComplete {
//        case Success(res) => {
//          println(res)
//          res.discardEntityBytes()
//          system.terminate()
//        }
//        case Failure(_)   => sys.error("something wrong")
//      }
  }
}


