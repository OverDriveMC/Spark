package com.gmail.mengcheng.sql
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

// Encoders are also created for case classes.
case class Person(name:String,age:Long)
object DataSetDemo {
  val conf=new SparkConf().setAppName("SparkContextSQL").setMaster("local")
  val sc=new SparkContext(conf)
  def main(args:Array[String]){
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val ds=Seq(1,2,3).toDS()
    // Returns: Array(2, 3, 4)
    ds.map(_+1).collect()
    
    val dsp=Seq(Person("Andy",32)).toDS()
    // DataFrames can be converted to a Dataset by providing 
    //a class. Mapping will be done by name.
    val path="D:/testfile/people.json"
    val people=sqlContext.read.json(path).as[Person]
    people.show()
  }
}