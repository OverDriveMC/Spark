package com.gmail.mengcheng.sql
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode


object LoadingData {
  val conf = new SparkConf().setAppName("Manually").setMaster("local")
  val sc = new SparkContext(conf)
  
  def main(args: Array[String]) {
    val path = "D:/devSoft/"
    System.setProperty("hadoop.home.dir", path)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    
    import sqlContext.implicits._
    val people=sqlContext.read.json("D:/testfile/sql/people.json").as[Person].rdd
    //隐式转换没转换成功
    // The RDD is implicitly converted to a DataFrame by implicits, 
    //allowing it to be stored using Parquet
    //people.toDF().write.parquet("D:/testfile/sql/person.parquet")
    val parquetFile=sqlContext.read.parquet("D:/testfile/sql/person.parquet")
    //Parquet files can also be registered as tables and then used in SQL statements.
    parquetFile.registerTempTable("parquetFile")
    val teenagers=sqlContext.sql("select name from parquetFile where age>=13 and age<=19")
    teenagers.map { x =>"name:"+x(0) }.collect().foreach(println)
  }
}