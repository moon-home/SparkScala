package yma27gitbook

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object SpendByCustomer {
  def parseLine(line:String)={
    val fields = line.split(",")
    (fields(0), fields(2).toFloat)
  }
  
  def main(args:Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "SpendByCustomer")
    val lines = sc.textFile("../customer-orders.csv")
    val rdd = lines.map(parseLine)
    val sbc = rdd.reduceByKey((x,y)=>x+y).map( x => (x._2, x._1) ).sortByKey()
    val res = sbc.collect()
    res.foreach(println)
  }
}