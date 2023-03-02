package spark_learning

import io.netty.handler.codec.http.HttpHeaders.getHost
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.text.SimpleDateFormat
import java.net.URL

object test extends App{
  val s= "https://rever.vn/du-an/lovera-vista-khang-dien?utm_campaign=Lovera%20Vista&utm_source=adgo&utm_medium=facebook"
  val url = new URL(s)
  println(url.getPath.split("/")(2))
  println(url.getHost)
  println(s.split("/")(4))
  val spark = SparkSession
  .builder()
  .appName("SomeAppName")
  .config("spark.master", "local")
  .getOrCreate();
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  val arrayStructureData = Seq(
    Row("James,,Smith",List("Java","Scala","C++"),"CA"),
    Row("Michael,Rose,",List("Spark","Java","C++"),"NJ"),
    Row("Robert,,Williams",List("CSharp","VB","R"),"NV")
  )

  val arrayStructureSchema = new StructType()
    .add("name",StringType)
    .add("languagesAtSchool", ArrayType(StringType))
    .add("currentState", StringType)

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
  import spark.implicits._
  df.show()

  //flatMap() Usage
  val df2=df.flatMap(
    f => {
    val lang=f.getSeq[String](1)
    println(lang)
    lang.map(println)
    lang.map((f.getString(0),_,f.getString(2)))
  })

//  val df3=df2.toDF("Name","language","State")
  df2.show(false)
}
