package com.gmail.mengcheng
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Matrix,Matrices}
import org.apache.spark.mllib.linalg.distributed.{RowMatrix,IndexedRow,IndexedRowMatrix}


object DataType {
  val conf=new SparkConf().setMaster("local[2]").setAppName("DataType")
  val sc=new SparkContext(conf)
  
  def main(args:Array[String]){
    val path = "D:/devSoft/"
    System.setProperty("hadoop.home.dir", path)
    //dataTypes()
    //sparseData()
    //rowMatrix()
    indexedRowMatrix()
  }
  def indexedRowMatrix(){
    val rows:RDD[IndexedRow]=sc.parallelize(Vector(IndexedRow(1,Vectors.dense(1,0,0)),
                IndexedRow(4,Vectors.dense(1,1,1)),
            IndexedRow(3,Vectors.dense(1,1,1)),
        IndexedRow(2,Vectors.dense(1,1,0)) ) ) 
    val mat:IndexedRowMatrix =new IndexedRowMatrix(rows)
    //5?why
    val m=mat.numRows()
    val n=mat.numCols()
    mat.rows.foreach(println)
    println(mat.rows.count())
    val rowMat:RowMatrix=mat.toRowMatrix()
    println(m+"  "+n)
    rowMat.rows.foreach(println)
    println(rowMat.numRows())
  }
  
  def rowMatrix(){
    // an RDD of local vectors
    val rows:RDD[Vector]=sc.parallelize(
        Vector(Vectors.dense(1,0,0),Vectors.dense(1,1,0),
            Vectors.dense(1,1,1),Vectors.dense(1,1,1)))
    // Create a RowMatrix from an RDD[Vector].
    val mat:RowMatrix=new RowMatrix(rows)
    // Get its size.
    val m=mat.numRows()
    val n=mat.numCols()
    // QR decomposition 
    val qrResult=mat.tallSkinnyQR(true)
    println(m+"   "+n)
    println("qrResult:")
   
    qrResult.Q.rows.foreach(println)
    println(qrResult.R)
  }
  
  def localMatrix(){
    val dm:Matrix =Matrices.dense(3,2,Array(1.0,3.0,5.0,2.0,4.0,6.0))
    val sm:Matrix=Matrices.sparse(3,2,Array(0,1,3),Array(0,2,1),Array(9,6,8))
    
  }
  
  def sparseData(){
    val examples:RDD[LabeledPoint]=MLUtils.loadLibSVMFile(sc, "D:/data/mllib/sample_libsvm_data.txt")
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