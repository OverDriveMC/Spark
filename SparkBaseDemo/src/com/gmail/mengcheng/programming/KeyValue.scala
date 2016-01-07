package com.gmail.mengcheng.programming
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.Sorting
import scala.math.Ordering

object KeyValue {
  var conf=new SparkConf().setAppName("keyValue").setMaster("local")
  var sc=new SparkContext(conf)
  def main(args:Array[String]){
    val lines=sc.textFile("D:/testfile/data2.txt")
    val flatMap=lines.flatMap { x => x.split("\t")}
    flatMap.foreach(println)
    val pairs=flatMap.map(s=>(s,1))
    val counts=pairs.reduceByKey((a,b)=>a+b)
    counts.foreach(println)
    
    val sorts=pairs.sortByKey()
    sorts.foreach(println)
    
    val sorts2=counts.sortByKey()
    sorts2.collect.foreach(println)
    
    ///可以通过定义这个修改默认排序规则
    ///该怎么将这个定义为临时的？？？？
    implicit val sortString=new Ordering[String]{
      override def compare(a : String , b:String)={
        a.length()- b.length()  
      }
    }
    ///这里的key为String，将按照上面定义的排序规则来比较
    val sorts3=counts.sortByKey()
    sorts3.collect.foreach(println)
   
    //定义比较规则
    val sorts4=counts.sortBy(a=>a._1.length()/a._2,true,1)
    sorts4.collect.foreach(println)
  }
}