import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.count

object test {
  def main(args: Array[String]): Unit ={
    val spark:SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val df1 = Seq(("James","Sales",34), ("Michael","Sales",56),
      ("Robert","Sales",30), ("Maria","Finance",24) ).toDF("name","job","age")
      val df2 = Seq(("James","150"),("Jack","160")).toDF("name","height")
    val finalDf = df2
      .join(df1,Seq("name"),"outer")
      .show(10)
  }
}
