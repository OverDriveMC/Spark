package com.gmail.mengcheng.programming
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object closure {
  val conf = new SparkConf().setAppName("closure").setMaster("local")
  val sc = new SparkContext(conf)

  def test1() {
    val data = Array(1, 2, 3, 4, 5)
    var counter = 0
    var counter2=sc.accumulator(0)
    var rdd = sc.parallelize(data)
    //wrong:don't do this!!
    rdd.foreach { x => 
      counter += x 
      counter2+=x
      println("counter:"+counter+"  "+x);  
    }
    println("Counter value:" + counter)
    println("Counter2 value:" + counter2)
  }
  def test2(){
    val data=Array(1,2,3,4,5)
    var rdd=sc.parallelize(data)
    rdd.collect().map(println)
    rdd.take(4).foreach(println)
  }
  
  def main(args: Array[String]) {
    //test1()
    test2()
  }
}