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
      * ����д��ͨ�����˵ģ���Ϊ�������ִ��룬����зֲ�ʽ���㣬�����ڲ����붼Ҫ�ܹ����л�
      * ��PrintWriter�����ܹ����л����������д�ڱհ��ڲ��ǿ��Եģ���Ϊд�ڱհ��ڲ����ھֲ�������
      * ��ÿ���������е�ʱ�򶼿��Դ���������Ӱ��������㣬��д������Ļ���ȫ�ֱ���
      * ����Ҫ����RDD�ļ���ʱ��ʹ��saveAsTextFile()���ڲ�RDD��ʲô��ʽ���ͱ���Ϊʲô��ʽ
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