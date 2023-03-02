package parquet_reader
import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, udf}
import rever.etl.rsparkflow.utils.JsonUtils.{toJson, toJsonNode}
object userReader {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val addFullNameUdf = udf((properties:String)=>{
      val json = toJsonNode(properties)
      json.get("full_name") match {
        case null =>"unknown"
        case value=> value.toString.drop(1).dropRight(1)
      }
    })
    val addEmailUdf = udf((properties:String)=>{
      val json = toJsonNode(properties)
      json.get("work_email") match {
        case null => "unknown"
        case value => value.toString.drop(1).dropRight(1)
      }
    })
    val addEmploymentStatusUdf = udf((properties:String)=>{
      val json = toJsonNode(properties)
      json.get("employment_status") match {
        case null => "unknown"
        case value => value.toString
      }
    })
    val df= spark.read.parquet("/home/maihuy/Downloads/user_2.parquet")
    df.printSchema()
    df.show(10,false)
    val filteredDf = df.filter(_.getAs[String]("status")!="0")
    filteredDf.createOrReplaceTempView("user_agent")
    val resultDf =
      spark.sql(initQuery())
        .withColumn("full_name",addFullNameUdf(col("properties")))
        .withColumn("email",addEmailUdf(col("properties")))
        .withColumn("employment_status",addEmploymentStatusUdf(col("properties")))
        .select(
          col("user_id"),
          col("full_name"),
          col("email"),
          col("account_type"),
          col("user_type"),
          col("job_title"),
          col("market_center_id"),
          col("team_id"),
          col("employment_status")
        )
    resultDf.printSchema()
    resultDf.show(false)

//    resultDf
//      .coalesce(1)
//      .write
//      .mode(SaveMode.Overwrite)
//      .format("csv")
//      .options(Map("header"->"true", "escape" -> "\""))
//      .csv("output/user_output")
    val userDf = spark.read
      .options(Map("header"->"true", "escape" -> "\""))
      .csv("output/user_output/*.csv")
    userDf.show(false)
  }
  def initQuery():String={
    s"""
       |SELECT
       |   username as user_id,
       |   account_type,
       |   user_type,
       |   job_title,
       |   properties,
       |   market_center_id,
       |   team_id,
       |   status
       |FROM user_agent
       |""".stripMargin
  }
}
