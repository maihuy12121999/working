package contact_checker

import org.apache.spark.sql.SparkSession
import rever.etl.rsparkflow.utils.JsonUtils.{toJson, toJsonNode}
object Contact {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val contactDf = spark
      .read
      .options(Map("header"->"true","escape"->"\""))
      .csv("/home/maihuy/Downloads/contact.csv")
    var d = Map.empty[String,Any]
    contactDf.foreach{
      row=>
        val properties = row.getAs[String]("properties")
        val propertiesJson = toJsonNode(properties)
    }
  }
}
