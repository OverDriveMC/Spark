package com.gmail.mengcheng.sql
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object SparkContextDemo {
  val conf=new SparkConf().setAppName("SparkContextSQL").setMaster("local")
  val sc=new SparkContext(conf)
  def main(args : Array[String]){
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)
    
    import sqlContext.implicits._
    val df=sqlContext.read.json("D:/testfile/people.json");
    /**
     * 貌似是每个类信息独占一行，不然挺奇怪
     */
    /*
      +---+----+
      |age|name|
      +---+----+
      | 15|  bm|
      | 18|  mk|
      | 10|  dh|
      +---+----+
     */
    df.show()
    /**
     * root
 				|-- age: string (nullable = true)
 				|-- name: string (nullable = true)
     */
    df.printSchema()
    //select only the "name" column
    df.select("name").show()
    // Select everybody, but increment the age by 1
    df.select(df("name"), df("age")+1).show()
    // Select people older than 15
    df.filter(df("age")>15).show()
    // Count people by age
    df.groupBy("age").count().show()
  }
}