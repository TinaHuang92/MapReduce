import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.io.{BufferedWriter, FileWriter}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import au.com.bytecode.opencsv.CSVWriter


/*
 written by Tingting Huang 2018/1/23 Tue
 */

object Tingting_Huang_task1 {

  def jsonStrToMap(jsonStr: String): Map[String, Any] = {
    implicit val formats = org.json4s.DefaultFormats

    parse(jsonStr).extract[Map[String, Any]]
  }

  def main (args : Array[String]) : Unit = {

    // local[*] Run Spark locally with as many worker threads as logical cores on your machine.
    val conf = new SparkConf().setAppName("Tingting_Huang_task1").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //val data = sc.textFile("file:/Users/huangtingting/Desktop/553/Assignment_01/reviews_Toys_and_Games_5.json")
    val data = sc.textFile(args(0))

    //testing
    //val test = data.take(1)(0)
    //println(jsonStrToMap(test)("reviewerID"))

    val input = data.map(jsonStrToMap).map(d => (d("asin").asInstanceOf[String], d("overall").asInstanceOf[Double]))

    val mapper = input.mapValues(mark => (mark, 1))
    val reducer = mapper.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val average = reducer.map { x =>
                                val tmp = x._2
                                val sum = tmp._1
                                val count = tmp._2
                                (x._1, sum / count)
    }.sortBy(x => x._1, true)
    //average.foreach( x => println(x) )

    //val output = new BufferedWriter(new FileWriter("./Tingting_Huang_result_task1.csv"))
    val output = new BufferedWriter(new FileWriter(args(1)))
    val csv = new CSVWriter(output)
    val csvSchema = Array("asin", "rating_avg")
    val record = average.map(x => Array(x._1.toString, x._2.toString)).collect().toList
    val listOfRecords = csvSchema +: record
    csv.writeAll(listOfRecords)
    output.close()

  }
}
