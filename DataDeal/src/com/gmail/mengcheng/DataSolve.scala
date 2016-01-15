package com.gmail.mengcheng
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import  org.apache.spark.rdd._
import java.io._
import java.text.SimpleDateFormat
import java.math.BigDecimal

object DataDeal {
    val conf=new SparkConf().setAppName("dataDeal").setMaster("local")
    val sc=new SparkContext(conf)
    val sdf = new SimpleDateFormat("yyyy/MM/dd H:mm")
    
    def main(args:Array[String]){
      val path="D:/devSoft/"
      System.setProperty("hadoop.home.dir",path)

      //selectData()
      filterData()
      
    }
    //选取数据
    def selectData(){
      for(i<-1 to 4){
        val file=sc.textFile("D:/testfile/databackup/data"+i+".txt")
        
        val writer = new PrintWriter(new File("D:/testfile/dataFilter/data"+i+".txt" ))
        file.take(1000).foreach { x =>
          writer.write(x+"\r\n")
        }
        
        writer.close()
      }
    }
    //过滤数据
    def filterData(){
      val file=sc.textFile("D:/testfile/datafinal/power/*.txt")
      val pairs=file.map { x =>  
        val tmp=x.split("\t")
        (tmp(0),tmp(1))
      }
      val counts=pairs.reduceByKey((a,b)=>(a+"~"+b))
      val fileData=counts.filter(x=>x._2.split("~").length==4);
     
      val aggData=fileData.map(x=>{
       val tmp=x._2.split("~")
       var res=0.0
       for(i<-0 to 3){
         res+=tmp(i).toDouble
       }
       (x._1,res +"")
      })
      
      val sdf2 = new SimpleDateFormat("yyyy/MM/dd H")
      val df   =new   java.text.DecimalFormat("#.00"); 
      
     // aggData.sortBy(x=>sdf.parse(x._1),true, 1).foreach(println)
      
      
      val aggHourData=aggData.map{
        x=>
          (x._1.substring(0,x._1.indexOf(":")),x._2)
      }.reduceByKey((a,b)=>(a+"~"+b))
      
 
      
      
      
      aggHourData.map{
        x=>
          val tmp=x._2.split("~")
          var res=0.0
          for(i<-0 to tmp.length-1){
            res+=tmp(i).toDouble
          }
         (x._1,res/tmp.length)
      }.sortBy(x=>sdf2.parse(x._1),true, 1).foreach{
        x=>
          val   b   =   new   BigDecimal(x._2.toDouble/4);  
          val   f1   =   b.setScale(2,   BigDecimal.ROUND_HALF_UP).doubleValue();  
          val index=x._1.indexOf(" ")
          val str=x._1.substring(0,index)+"\t" +
            x._1.substring(index+1)+"\t" + f1
          println(str)
      }
      
      
    }
    
    
}

