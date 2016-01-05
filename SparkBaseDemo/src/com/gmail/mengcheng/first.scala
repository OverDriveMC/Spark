package com.gmail.mengcheng

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
class first{
}

object first{
  val conf=new SparkConf().setAppName("first").setMaster("local")
  val sc=new SparkContext(conf)
  def main(args : Array[String]):Unit={
//    testCollection()
//    testFileSystem()
//    testWholeFile()
    testPassFunction()
  }
  
  def testCollection(){

    val data=Array(1,2,3,4,5)
    val distData=sc.parallelize(data)
    val value=distData.reduce((a,b)=>a+b)
    print(value)
  }
  def testFileSystem(){
    //本地的话似乎直接使用目录不行
    val distFile=sc.textFile("D:/testfile/*.txt")
    distFile.foreach(println)
    val totalLength=distFile.map { a =>a.length() }.reduce( (a,b) =>a+b)
    print("totalLength:"+totalLength)
  }
  def testWholeFile(){
    val distFile=sc.wholeTextFiles("D:/testfile/*")
    distFile.map(a=>a._1+":"+a._2).foreach(println)
  }
  
  def testPassFunction(){
    val data=Array("hello","world")
    val rdd=sc.parallelize(data)
    //pass function
    rdd.map(MyFunctions.func1).foreach(println)
  }
}


object MyFunctions{
  def func1(s:String):String={
    s+":"+s.length()
  }
}