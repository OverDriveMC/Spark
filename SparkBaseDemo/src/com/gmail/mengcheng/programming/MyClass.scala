package com.gmail.mengcheng.programming
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

class MyClass {
  //有返回值的话需要加 =貌似。。。
  def func1(s :String) :String={
     s+":"+s.length()
  } 
  def doStuff(rdd : RDD[String]):RDD[String]={
    rdd.map(func1)
  }
  
  val field="Hello"
  def doStuff2(rdd : RDD[String]):RDD[String]={
    rdd.map(x=>field+x)
  }
  
  def doStuff3(rdd:RDD[String]):RDD[String]={
    val field_ = this.field
    rdd.map(x=> field_ + x)
  }
}