package com.gmail.mengcheng
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Matrix,Matrices}
import org.apache.spark.mllib.linalg.distributed.{RowMatrix,IndexedRow,IndexedRowMatrix}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary,Statistics}

object StatisticsDemo {
  val conf=new SparkConf().setMaster("local[2]").setAppName("DataType")
  val sc=new SparkContext(conf)
  
  def main(args:Array[String]){
    val path = "D:/devSoft/"
    System.setProperty("hadoop.home.dir", path)
    testColstats()
  }
  def testColstats(){
    val observations:RDD[Vector]=sc.parallelize(
        Vector(Vectors.dense(1,2,3),Vectors.dense(5,2,3),
            Vectors.dense(3,2,1),
            Vectors.dense(5,2,4)))
    val summary:MultivariateStatisticalSummary=
          org.apache.spark.mllib.stat.Statistics.colStats(observations)
    println(summary.mean)
    println(summary.variance)
    println(summary.numNonzeros)
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
}