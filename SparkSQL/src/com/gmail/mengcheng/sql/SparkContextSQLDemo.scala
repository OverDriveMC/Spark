package com.gmail.mengcheng.sql
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object SparkContextDemo {
  val conf=new SparkConf().setAppName("SparkContextSQL").setMaster("local")
  val sc=new SparkContext(conf)
  def main(args : Array[String]){
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)
    
    import sqlContext.implicits._
  }
}