package com.gmail.mengcheng.programming
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.AccumulatorParam

object Accmulator {
  val conf=new SparkConf().setAppName("broadcast")
          .setMaster("local")
  val sc=new SparkContext(conf)
  
  def main(args : Array[String]){
    val accum=sc.accumulator(0,"accumulator")
    val data=sc.parallelize(Array(1,2,3,4))
    data.foreach { x => accum+=x }
    println(accum.value)
  }
}

