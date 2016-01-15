package com.gmail.mengcheng
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat

object Difference {
      val conf=new SparkConf().setAppName("dataDeal").setMaster("local")
      val sc=new SparkContext(conf)
      val eps=0.1
      
      def main(args:Array[String]){
        val path="D:/devSoft/"
        System.setProperty("hadoop.home.dir",path)
        
        val file=sc.textFile("D:/testfile/datafinal/res/*.txt")
        val pairs=file.map{
          x=>
            val tmp=x.split("\t")
            (tmp(0)+" "+tmp(1),tmp(2)+"\t"+tmp(3))
        }
        val allData=pairs.reduceByKey((a,b)=>(a+"~"+b))
        val realData=allData.filter(x=>x._2.split("~").length==2)
        val sdf2 = new SimpleDateFormat("yyyy/MM/dd H")
        
        
        realData.sortBy(x=>sdf2.parse(x._1),true,1).foreach{
          x=>
            val tmp=x._2.split("~")
            val wind0=tmp(0).split("\t")(0)
            val power0=tmp(0).split("\t")(1)
            val wind1=tmp(1).split("\t")(0)
            val power1=tmp(1).split("\t")(1)
            if( ( (wind0.toDouble-wind1.toDouble)<eps
                &&(wind1.toDouble-wind0.toDouble)<eps)
                
                
                || ((power0.toDouble-power1.toDouble)<eps
                   &&  ( (power1.toDouble-power0.toDouble)<eps)
                
                
            )){
              
            }else{
              println(x._1)
            }
        }
        
      }
}