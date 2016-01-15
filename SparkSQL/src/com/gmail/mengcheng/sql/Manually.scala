package com.gmail.mengcheng.sql
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Manually {
  val conf = new SparkConf().setAppName("Manually").setMaster("local")
  val sc = new SparkContext(conf)
  def main(args: Array[String]) {
    val path = "D:/devSoft/"
    System.setProperty("hadoop.home.dir", path)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val df = sqlContext.read.format("json").load("D:/testfile/sql/people.json")
    df.show()
    /*
    df.select("name", "age").write.format("parquet").
          save("D:/testfile/sql/nameAndAges.parquet")
		*/
    val df2=sqlContext.sql("select * from parquet.`D:/testfile/sql/nameAndAges.parquet`")
    df2.show()
  }
}