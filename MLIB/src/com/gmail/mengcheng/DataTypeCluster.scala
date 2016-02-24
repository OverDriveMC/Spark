package com.gmail.mengcheng
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object DataTypeCluster {
  val conf=new SparkConf().setMaster("spark://192.168.126.10:7077").setAppName("DataType")
  val sc=new SparkContext(conf)
  
  def main(args:Array[String]){
    dataTypes()
    sparseData()
  }
  def sparseData(){
    val examples:RDD[LabeledPoint]=MLUtils.loadLibSVMFile(sc, "hdfs://192.168.126.10:9000/user/mc/data/mllib/sample_libsvm_data.txt")
    examples.foreach(println)
  }
  
  def labeledPoint(){
    // Create a labeled point with a positive label and a dense feature vector.
    val pos=LabeledPoint(1.0,Vectors.dense(1.0,0.0,3.0))
    // Create a labeled point with a negative label and a sparse feature vector.
    val neg=LabeledPoint(0.0,Vectors.sparse(3,Array(0,2),Array(1.0,3.0)))
  }
  
  def dataTypes(){
    // Create a dense vector (1.0, 0.0, 3.0).
    val dv:Vector=Vectors.dense(1.0,0.0,3.0)
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying 
    //its indices and values corresponding to nonzero entries.
    val sv1:Vector=Vectors.sparse(3,Array(0,2),Array(1.0,3.0))
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its nonzero entries.
    val sv2:Vector=Vectors.sparse(3,Seq((0,1.0),(2,3.0)))
  }
}