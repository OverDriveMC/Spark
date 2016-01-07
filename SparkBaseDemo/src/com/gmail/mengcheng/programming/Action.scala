package com.gmail.mengcheng.programming
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Action {
  var conf=new SparkConf().setAppName("transformation").setMaster("local")
  var sc=new SparkContext(conf)
  def main(args:Array[String]){
    val path="D:/devSoft/"
    System.setProperty("hadoop.home.dir",path)
    
    //reduce(func)
    val data=sc.parallelize( Array(3,2,4,1,5) )
    println(data.reduce((a,b)=>a+b))
    
    //count()
    println(data.count())
    
    //take
    data.take(3).foreach(println)
    
    //takeOrdered
    data.takeOrdered(5).foreach(println)
    
    //saveAsTextFile(path)
    //data.coalesce(1,true).saveAsTextFile("file:/D:/testfile/data4")
    
    val keyValueData=sc.textFile("D:/testfile/data.txt")
    val counts=keyValueData.flatMap(x=>x.split("\t")).map { x => (x,1) }.
          reduceByKey((a,b)=>(a+b))
    
    //saveAsSequenceFile(path) 
    counts.saveAsSequenceFile("file:/D:/testfile/sequenceData")
    
    //sequenceFile
    val readSequenceFile=
      sc.sequenceFile[org.apache.hadoop.io.Text,
        org.apache.hadoop.io.Text]("file:/D:/testfile/sequenceData")
    readSequenceFile.foreach(println)
  }
}