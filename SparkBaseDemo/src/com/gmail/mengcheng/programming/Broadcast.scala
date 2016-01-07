package com.gmail.mengcheng.programming
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.Sorting
import scala.math.Ordering
object Broadcast {
  val conf=new SparkConf().setAppName("broadcast")
          .setMaster("local")
  val sc=new SparkContext(conf)
  
  def main(args : Array[String]){
    val data=sc.broadcast(Array(1,2,3,4,5))
    data.value.foreach(println)
  }
}