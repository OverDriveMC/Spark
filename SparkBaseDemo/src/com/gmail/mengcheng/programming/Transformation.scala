package com.gmail.mengcheng.programming
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Transformation {
  var conf=new SparkConf().setAppName("transformation").setMaster("local")
  var sc=new SparkContext(conf)
  
  def main(args:Array[String]){
    val lines=sc.textFile("D:/testfile/data2.txt")
    val flatMap=lines.flatMap { x => x.split("\t") }
    val pairs=flatMap.map(x=>(x,1))
    val counts=pairs.reduceByKey((a,b)=>a+b)
    
    counts.collect.foreach(println)
    //filter return true 
    val sizeOne=counts.filter(x=>x._2==1)
    sizeOne.collect.foreach(println)
    //mapPartitions
    //参数返回值都是迭代器
    val mapPa=counts.mapPartitions(x=>x.map(y=>(y._1+"hehe",y._2)))
    mapPa.foreach(println)
    //union
    sizeOne.union(sizeOne).foreach(println)
    
    //groupByKey(K, Iterable<V>) 
    val res=pairs.groupByKey()
    res.foreach{
      x=>println("key:"+x._1+" value:"+x._2.foreach(println))
    }
    
    //aggregateByKey(zeroValue)(seqOp, combOp, [numTasks])
    val aggRes=pairs.aggregateByKey(0)(aggregateFun,aggregateFun)
    aggRes.foreach(println)
    
    //join(otherDataset, [numTasks])  (K, V) and (K, W) =>(K, (V, W))
    val joinRes=counts.join(counts)
    joinRes.foreach(println)
    
    //cartesian(otherDataset)
    val data=sc.parallelize(Array(1,2,3,4,5))
    flatMap.cartesian(data).foreach(println)
  }
  
  def aggregateFun(x1 : Int , x2: Int):Int={
    x1+x2
  }
}