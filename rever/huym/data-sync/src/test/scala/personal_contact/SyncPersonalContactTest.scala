package personal_contact

import org.apache.spark.sql.functions.{col, countDistinct}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.scalatest.FunSuite

class SyncPersonalContactTest extends FunSuite {
  SparkSession
    .builder()
    .master("local[2]")
    .getOrCreate()

  private val pContactDataMartDf = readPContactDataMart()

  private val pContactDbDf = readPContactDB()

  test("DataMart miss contact from Main Storage") {

    val df = pContactDataMartDf
      .join(
        pContactDbDf,
        pContactDataMartDf("p_cid") === pContactDbDf("p_cid"),
        "leftanti"
      )
      .persist(StorageLevel.MEMORY_AND_DISK_2)

    exportDf(df, "not_in_database")

  }

  test("Main Storage miss contact from DataMart") {

    val df = pContactDbDf
      .join(
        pContactDataMartDf,
        pContactDbDf("p_cid") === pContactDataMartDf("p_cid"),
        "leftanti"
      )
      .persist(StorageLevel.MEMORY_AND_DISK_2)

    exportDf(df, "not_in_data_mart")

    df.select(countDistinct(col("p_cid"))).show()
  }

  private def exportDf(df: DataFrame, name: String): Unit = {
    df.coalesce(1)
      .write
      .format("csv")
      .options(Map("header" -> "true", "delimiter" -> ","))
      .mode(SaveMode.Overwrite)
      .save(s"data/output/${name}.csv")

  }

  private def readPContactDataMart(): DataFrame = {
    SparkSession.active.read
      .options(Map("header" -> "true", "delimiter" -> ","))
      .csv("data/DataMart_Personal_Contact.csv")
      .withColumn("p_cid", col("p_cid").cast(StringType))
  }

  private def readPContactDB(): DataFrame = {
    SparkSession.active.read
      .options(Map("header" -> "true", "delimiter" -> ","))
      .format("csv")
      .csv("data/pcontact_id_1511_2011.csv")
  }

}
