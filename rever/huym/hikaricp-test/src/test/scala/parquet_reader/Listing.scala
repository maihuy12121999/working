package parquet_reader

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object Listing {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val month9Df = spark.read.parquet("/home/maihuy/Downloads/total_publish_30_09.parquet")
    month9Df.show()
    val month10Df = spark.read.parquet("/home/maihuy/Downloads/total_publish_19_10.parquet")
    month10Df.show(false)
    val publishedMonth10Df = month10Df.join(month9Df,month10Df("property_id")===month9Df("property_id"),"leftanti")
    val listingDf = spark
      .read
      .option("header",value = true)
      .csv("/home/maihuy/Downloads/listing.csv")
    listingDf.show()
    val resultDf = publishedMonth10Df
      .joinWith(listingDf,publishedMonth10Df("property_id")===listingDf("property_id"),"inner")
      .select(
        col("_1.property_id"),
        col("_2.market_center").as("market_center_id"),
        col("_2.RVID")
      )
    resultDf
          .coalesce(1)
          .write
          .mode(SaveMode.Overwrite)
          .format("csv")
          .options(Map("header"->"true", "escape" -> "\""))
          .csv("output/listing_output")
  }
}
