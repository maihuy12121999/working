package spark_learning

import org.apache.spark.sql.SparkSession
import spark_learning.SparkTest.spark

import scala.Array.canBuildFrom

case class Person(name:String, age:Long)
object SQLPrograming extends App {
  val spark = SparkSession
    .builder()
    .appName("SomeAppName")
    .config("spark.master", "local")
    .getOrCreate();
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  val df = spark.read.json("input/people.json")
  //  =========================Untyped Dataset Operations (aka DataFrame Operations)==========
  df.show()
  df.printSchema()
  df.select("name").show()
  df.select($"name", $"age" + 1).show()
  df.filter($"age" > 21).show()
  df.groupBy("age").count().show()
  //  ==========================Running SQL Queries Programmatically=============================
  df.createOrReplaceTempView("people")
  val sqlDF = spark.sql("SELECT * from people")
  sqlDF.show()
  //  ========================Global Temporary View===============================================
  df.createGlobalTempView("people")
  spark.sql("select * from global_temp.people").show()
  spark.newSession().sql("select * from global_temp.people").show()
  //  =================Creating Datasets=======================
  val caseClassDS = Seq(Person("Mai Huy", 24)).toDS()
  caseClassDS.show()
  val primitiveDS = Seq(1, 2, 3).toDS()
  primitiveDS.map(_ + 1).show()
  val path = "input/people.json"
  val peopleDS = spark.read.json(path).as[Person]
  peopleDS.show()
  //  ========================Inferring the Schema Using Reflection========================
  val peopleDF = spark.sparkContext.textFile("input/people.txt").map(_.split(","))
    .map(array => Person(array(0), array(1).trim.toInt))
    .toDF()
  peopleDF.createOrReplaceTempView("people")
  val teenagerDF = spark.sql("Select name, age from people where age between 20 and 30")
  println("================================DAy ne========================")
  teenagerDF.map(teenager => "Name: " + teenager(0)).show()
  teenagerDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
  implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
  val mapTeenagerDF = teenagerDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
  println(mapTeenagerDF.mkString("Array(", ", ", ")"))
    //  ============================Programmatically Specifying the Schema=======================

  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types._

  val peopleRDD = spark.sparkContext.textFile("input/people.txt")
  val schemaString = "name age"
  val fields = schemaString.split(" ")
    .map(fieldName => StructField(fieldName, StringType,nullable = true))
  val schema = StructType(fields)
  val rowRDD = peopleRDD.map(_.split(","))
    .map(attributes => Row(attributes(0),attributes(1).trim))
  val peopleDataFrame = spark.createDataFrame(rowRDD,schema)
  peopleDataFrame.createOrReplaceTempView("people")
  val results = spark.sql("select name from people")
  results.map(attributes=>"Name: "+ attributes(0)).show()
}