package spark_learning
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.sql.SparkSession

import java.lang.Math.{max,min}
import scala.language.postfixOps
object SparkTest extends App{
  val spark = SparkSession
    .builder()
    .appName("SomeAppName")
    .config("spark.master", "local")
    .getOrCreate();
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  val textFile = spark.read.textFile("input/outlast.txt")
  println(textFile.count())
  println(textFile.first())
  val lineContainString = textFile.filter(_.contains("Mai Huy"))
  val lineMostWords = textFile.map(_.split(" ").length)
  println(lineMostWords.reduce((a,b)=>min(a,b)))
  println(lineMostWords.show())
  val wordCounts = textFile.map(line => line.split(" ")).groupByKey(identity).count()
  val arrayWordCounts = wordCounts.collect()
  println(arrayWordCounts.mkString("Array(", ", ", ")"))
}
