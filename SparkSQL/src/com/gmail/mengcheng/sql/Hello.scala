package com.gmail.mengcheng.sql
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object Hello {
  val conf = new SparkConf().setMaster("local").setAppName("JSON Datasets")
  val sc = new SparkContext(conf)
  def main(args : Array[String]){
    val data = sc.parallelize(List(("a",1,3),("a",1,2),("b",1, 4),("b",2,3)))
    data.foreach(println)
    val counts=data.map(x=>(x._1,(x._2,x._3))).reduceByKey((a,b)=>(a._1+b._1,a._2+b._2))
    counts.map(x=>(x._1,x._2._1,x._2._2)).foreach(println)
  }
}