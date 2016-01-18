package com.gmail.mengcheng.sql
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

// Define the schema using a case class.
// Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
// you can use custom classes that implement the Product interface.
case class People(name:String,age:Int)

object Reflection {
  val conf=new SparkConf().setAppName("convert reflection").setMaster("local")
  val sc=new SparkContext(conf)
  def main(args : Array[String]){
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)
   // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    // Create an RDD of Person objects and register it as a table.
    val people=sc.textFile("D:/testfile/people.txt").map(_.split(",")).
                  map(p=>People(p(0),p(1).trim.toInt)).toDF()
    people.registerTempTable("people") 
    people.show()
    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers=sqlContext.sql("select name,age from people where age>=13 and age<=19")
    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by field index:
    teenagers.map { t => "Name:"+t(0) }.collect().foreach(println)
    // or by field name:
    teenagers.map(t=>"Name:"+t.getAs[String]("name")).collect().foreach(println)
    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
//    Map(name -> bm, age -> 15)
//    Map(name -> mk, age -> 16)
    //Any 类型是Scala 类型结构的根类型,类似于Object
    teenagers.map(_.getValuesMap[Any](List("name","age"))).collect().foreach(println)
        
  }
}