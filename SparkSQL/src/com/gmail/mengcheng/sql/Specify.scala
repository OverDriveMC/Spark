package com.gmail.mengcheng.sql
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Specify {
  val conf=new SparkConf().setAppName("convert reflection").setMaster("local")
  val sc=new SparkContext(conf)
  def main(args:Array[String]){
    val sqlContext=new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val people=sc.textFile("D:/testfile/people.txt")
    // The schema is encoded in a string
    val schemaString="name age"
    // Import Row.
    import org.apache.spark.sql.Row
    // Import Spark SQL data types
    import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType}
    // Generate the schema based on the string of schema
    
    val schema1=StructType(
        StructField("name",StringType,true) ::
        StructField("age",IntegerType,true) ::Nil)
    
    val schema=StructType(schemaString.split(" ").map(
        fieldName=>StructField(fieldName,StringType,true)       )
        )
    // Convert records of the RDD (people) to Rows.
    val rowRDD=people.map(_.split(",")).map { p => Row(p(0),p(1).trim().toInt) }
    // Apply the schema to the RDD.
    val peopleDataFrame=sqlContext.createDataFrame(rowRDD,schema1)
    // Register the DataFrames as a table.
    peopleDataFrame.registerTempTable("people")
    peopleDataFrame.show()
    peopleDataFrame.printSchema()
    // SQL statements can be run by using the sql methods provided by sqlContext.
    val results=sqlContext.sql("select * from people")
    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by field index or by field name.
    results.map { t => "Name:"+t(0)+"   age:"+t(1) }.collect().foreach(println)
  }
}