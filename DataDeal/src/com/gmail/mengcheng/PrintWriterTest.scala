package com.gmail.mengcheng
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.PrintWriter

object PrintWriterTest extends Serializable{

  def main(args : Array[String]){
     val path="D:/devSoft/"
     System.setProperty("hadoop.home.dir",path)
     val conf=new SparkConf().setAppName("test").setMaster("local")
     val sc=new SparkContext(conf)
     val file=sc.textFile("D:/testfile/data/1.txt")
     val pw=new PrintWriter("D:/testfile/data/2.txt")
     val counts=file.flatMap { x => x.split("\t") }.map(x=>(x,1)).reduceByKey(_+_)
     counts.foreach(println)
     /**
      * 这样写是通过不了的，因为对于这种代码，会进行分布式计算，所以内部代码都要能够序列化
      * 而PrintWriter本身不能够序列化，但是如果写在闭包内部是可以的，因为写在闭包内部属于局部变量，
      * 在每个分区运行的时候都可以创建，并不影响整体计算，而写在外面的话是全局变量
      * 所以要保存RDD文件的时候使用saveAsTextFile()，内部RDD是什么格式，就保存为什么格式
      */
     /**
     counts.foreach{
       x=>
         pw.write(x+"\r\n")
     }
     pw.close()
     */
     counts.map(x=>x._1+"-"+x._2).saveAsTextFile("D:/testfile/data/res");
  }
}