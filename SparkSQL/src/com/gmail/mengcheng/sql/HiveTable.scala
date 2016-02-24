package com.gmail.mengcheng.sql
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object HiveTable {
  val conf = new SparkConf().setMaster("local").setAppName("JSON Datasets")
  val sc = new SparkContext(conf)
  def main(args: Array[String]) {
    val winutilpath = "D:/devSoft/"
    System.setProperty("hadoop.home.dir", winutilpath)
    import org.apache.hadoop.fs._
    val path = new Path("file:/tmp/hive")
    val lfs = FileSystem.get(path.toUri(), sc.hadoopConfiguration)
    lfs.setPermission(path, new org.apache.hadoop.fs.permission.FsPermission("777"))
    println(lfs.getFileStatus(path).getPermission())
    

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    sqlContext.sql("create table if not exists src(key int,value string)")
    sqlContext.sql("load data local inpath"
      + "'D:/testfile/sql/data.txt' into table src")
    // Queries are expressed in HiveQL
    sqlContext.sql("from src select key.value").collect().foreach(println)
  }
}