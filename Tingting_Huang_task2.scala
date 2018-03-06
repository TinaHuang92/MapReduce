import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.json4s._
import org.json4s.jackson.JsonMethods._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.sql.SparkSession

import java.io.{BufferedWriter, FileWriter}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import au.com.bytecode.opencsv.CSVWriter

/*
 written by Tingting Huang 2018/1/24 Weds
 */

object Tingting_Huang_task2 {

  def jsonStrToMap(jsonStr: String): Map[String, Any] = {
    implicit val formats = org.json4s.DefaultFormats
    val mapper = new ObjectMapper()
    // Configure here
    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true) // allow single quotes
    mapper.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true)
    mapper.registerModule(DefaultScalaModule)
    mapper.readValue(jsonStr, classOf[Map[String, Any]])
  }

  def brandFilter(map: Map[String, Any]): Boolean = {
    if (map.contains("brand") && (map("brand") != null) && (map("brand") != "")) {
      return true
    }
    return false
  }

  def main (args : Array[String]) : Unit = {

    // local[*] Run Spark locally with as many worker threads as logical cores on your machine.
    val conf = new SparkConf().setAppName("Tingting_Huang_task2").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //val ratingData = sc.textFile("file:/Users/huangtingting/Desktop/553/Assignment_01/reviews_Toys_and_Games_5.json")
    //val brandData = sc.textFile("file:/Users/huangtingting/Desktop/553/Assignment_01/metadata.json")
    val ratingData = sc.textFile(args(0))
    val brandData = sc.textFile(args(1))

    //testing
    //val test = ratingData.take(1)(0)
    //println(jsonStrToMap(test)("reviewerID"))

    val ratingInput = ratingData.map(jsonStrToMap).map(d => (d("asin").asInstanceOf[String], d("overall").asInstanceOf[Double]))
    val brandInput = brandData.map(jsonStrToMap).filter(brandFilter).map(d => (d("asin").asInstanceOf[String], d("brand").asInstanceOf[String]))

    val joinRatingAndBrand = ratingInput.join(brandInput).map(x => (x._2._2, x._2._1))

    val mapper = joinRatingAndBrand.mapValues(mark => (mark, 1))
    val reducer = mapper.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val average = reducer.map { x =>
      val tmp = x._2
      val sum = tmp._1
      val count = tmp._2
      (x._1, sum / count)
    }.sortBy(x => x._1, true)//.collect()
    //average.foreach( x => println(x) )

    //val output = new BufferedWriter(new FileWriter("./Tingting_Huang_result_task2.csv"))
    val output = new BufferedWriter(new FileWriter(args(2)))
    val csv = new CSVWriter(output)
    val csvSchema = Array("brand", "rating_avg")
    val record = average.map(x => Array(x._1.toString, x._2.toString)).collect().toList
    val listOfRecords = csvSchema +: record
    csv.writeAll(listOfRecords)
    output.close()
  }

}
