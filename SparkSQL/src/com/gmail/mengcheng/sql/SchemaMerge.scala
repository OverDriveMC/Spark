package com.gmail.mengcheng.sql
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode

object SchemaMerge {
  val conf=new SparkConf().setAppName("schema merge").setMaster("local")
  val sc=new SparkContext(conf)
  def main(args:Array[String]){
        val path = "D:/devSoft/"
    System.setProperty("hadoop.home.dir", path)
    val sqlContext=new org.apache.spark.sql.SQLContext(sc);
    // This is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    // Create a simple DataFrame, stored into a partition directory
    val df1=sc.makeRDD(1 to 5).map { i =>(i, i*2) }.toDF("single","double")
    /**
     *  两个后缀名必须要一样，这个名称会成为合并之后的表的一列，值为几，该行就为几
     */
    df1.write.parquet("D:/testfile/sql/data/test_table/data=5")
    // Create another DataFrame in a new partition directory,
    // adding a new column and dropping an existing column
    val df2=sc.makeRDD(6 to 10).map(i=>(i,i*3)).toDF("single","tripe")
    df2.write.parquet("D:/testfile/sql/data/test_table/data=2")
    // Read the partitioned table
    val df3=sqlContext.read.option("mergeSchema","true").parquet("D:/testfile/sql/data/test_table")
    df3.printSchema()
    // The final schema consists of all 3 columns in the Parquet files together
    // with the partitioning column appeared in the partition directory paths.
    // root
    // |-- single: int (nullable = true)
    // |-- double: int (nullable = true)
    // |-- triple: int (nullable = true)
    // |-- data : int (nullable = true)
    df3.show()
/*
 +------+-----+------+----+
|single|tripe|double|data|
+------+-----+------+----+
|     1| null|     2|   5|
|     2| null|     4|   5|
|     3| null|     6|   5|
|     4| null|     8|   5|
|     5| null|    10|   5|
|     6|   18|  null|   2|
|     7|   21|  null|   2|
|     8|   24|  null|   2|
|     9|   27|  null|   2|
|    10|   30|  null|   2|
+------+-----+------+----+
 */
  }
}