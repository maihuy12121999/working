package spark_learning.reader

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.language.postfixOps

object ReadWriteParquet {
  def main(args: Array[String]): Unit = {
    //  ===========================  Parquet =====================================
    val spark = SparkSession
      .builder()
      .appName("SomeAppName")
      .config("spark.master", "local[4]")
      .getOrCreate();

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val squareDF  = spark.sparkContext.makeRDD(1 to 5).map(i=>(i,i*i)).toDF("value","square")
    writeParquet(squareDF,"output/test_table/key=1")
    val cubeDF  = spark.sparkContext.makeRDD(1 to 5).map(i=>(i,i*i*i)).toDF("value","cube")
    writeParquet(cubeDF,"output/test_table/key=2")
//    val mergeDF = spark.read.option("mergeSchema","true").parquet("data/test_table")
    val mergeDF = readParquetWithOptions(spark,"output/test_table",Map("mergeSchema"->"true"))
    mergeDF.show(100)
  }
  def readParquet(spark: SparkSession,path:String): DataFrame = {
    val df = spark.read.parquet(path)
    df
  }
  def readParquetWithOptions(spark:SparkSession, path:String, mapOptions:Map[String,String]):DataFrame = {
    val df = spark.read.options(mapOptions).parquet(path)
    df
  }
  def writeParquet(df :DataFrame, path:String): Unit ={
    df.write.mode(SaveMode.Overwrite).parquet(path)
  }
}
