package spark_learning

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object MyFunctions{
  def computeLineLength(s:String):Int = s.length
}
object RDDPrograming extends App {
  val conf = new SparkConf().setAppName("RDD").setMaster("local")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  val data = Array(1, 2, 3, 4, 5)
  val disData = sc.parallelize(data, 10)
  println(disData)
  val distFile = sc.textFile("input/outlast.txt")
  println(distFile.map(MyFunctions.computeLineLength).persist().reduce((a, b) => a + b))
  //  distFile.collect().foreach(println)
  println("=========== Working with key value pairs ==========")
  val pairs = distFile.map(s => (s, 1))
  val counts = pairs.reduceByKey((a, b) => a + b)
  println(counts)
  val accum =   sc.longAccumulator("skt")
  sc.parallelize(Array(1,2,3,4,5)).foreach(x=>accum.add(x))
  println(accum.value)
}
