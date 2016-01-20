package com.gmail.mengcheng.sql
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object JSONDataSet {
  val conf=new SparkConf().setMaster("local").setAppName("JSON Datasets")
  val sc=new SparkContext(conf)
  def main(args : Array[String]){
    val winutilpath = "D:/devSoft/"
    System.setProperty("hadoop.home.dir", winutilpath)
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)
    // A JSON dataset is pointed to by path.
  // The path can be either a single text file or a directory storing text files.
    val path="D:/testfile/sql/people.json"
    val people=sqlContext.read.json(path)
    // The inferred schema can be visualized using the printSchema() method.
    people.printSchema()
    // Register this DataFrame as a table.
    people.registerTempTable("people")
    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenages=sqlContext.sql("select name from people where age>=13 and age<=19")
    teenages.foreach(println)
    // Alternatively, a DataFrame can be created for a JSON dataset represented by
    // an RDD[String] storing one JSON object per string.
    val anotherPeopleRDD=sc.parallelize(  """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val anotherPeople=sqlContext.read.json(anotherPeopleRDD)    
    anotherPeople.registerTempTable("another")
    val res=sqlContext.sql("select address.city from another")
    res.foreach(println)
  }
}