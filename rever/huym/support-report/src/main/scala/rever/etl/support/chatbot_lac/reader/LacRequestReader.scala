package rever.etl.support.chatbot_lac.reader

import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.rsparkflow.spark.api.SourceReader
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.utils.TimestampUtils

class LacRequestReader extends SourceReader {

  override def read(s: String, config: Config): Dataset[Row] = {
    val driver = config.get("CH_DRIVER")
    val host = config.get("CH_HOST")
    val port = config.getInt("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val password = config.get("CH_PASSWORD")

    val df = SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port/log")
      .option("inferSchema", "true")
      .option("user", userName)
      .option("password", password)
      .option("driver", driver)
      .option("query", buildQuery(config))
      .load()

    val propertyTypeUdf = udf((propertyType: Int) => {
      propertyType match {
        case 1  => "APARTMENT"
        case 2  => "SERVICE_APARTMENT"
        case 3  => "PENT_HOUSE"
        case 4  => "VILLA"
        case 5  => "LOFT_HOUSE"
        case 6  => "OFFICE_TEL"
        case 7  => "CONDO_TEL"
        case 8  => "TOWN_HOUSE"
        case 9  => "SHOP_HOUSE"
        case 10 => "VILLA_REST"
        case 11 => "STUDIO_FLAT"
        case 12 => "DUPLEX"
        case 16 => "PLOT"
        case 32 => "OFFICE"
        case 33 => "BUILDING_BUSINESS"
        case 34 => "LAND_BUSINESS"
        case 35 => "WAREHOUSE_WORKSHOP"
        case 36 => "INDUSTRIAL_LAND"
        case 64 => "OTHER"
        case _  => propertyType.toString
      }
    })

    df.withColumn("property_type", propertyTypeUdf(col("property_type")))
  }

  private def buildQuery(config: Config): String = {
    val (startYear, endYear) = TimestampUtils.toStartEndOfYear(config.getDailyReportTime)

    s"""
       |SELECT formatDateTime(toDateTime(`timestamp`/1000, 'Asia/Ho_Chi_Minh'), '%H:%M:%S', 'Asia/Ho_Chi_Minh') AS `time`,
       |       formatDateTime(toDateTime(`timestamp`/1000, 'Asia/Ho_Chi_Minh'), '%Y-%m-%d', 'Asia/Ho_Chi_Minh') AS `date`,
       |       formatDateTime(toDateTime(`timestamp`/1000, 'Asia/Ho_Chi_Minh'), '%Y-%m', 'Asia/Ho_Chi_Minh') AS `month`,
       |       formatDateTime(toDateTime(`timestamp`/1000, 'Asia/Ho_Chi_Minh'), '%Y', 'Asia/Ho_Chi_Minh') AS `year`,
       |       dictGetOrDefault('default.dict_staff', 'full_name', tuple(`username`), '') AS full_name,
       |       dictGetOrDefault('default.dict_staff', 'work_email', tuple(`username`), '') AS email,
       |       dictGetOrDefault('default.dict_team', 'name', tuple(assumeNotNull(team)), '') AS team_name,
       |       dictGetOrDefault('default.dict_team', 'name', tuple(assumeNotNull(market_center)), '') AS `market_center`,
       |       listing_id,
       |       listing_type,
       |       property_type,
       |       property_id,
       |       dictGetOrDefault('default.dict_projects', 'name', tuple(assumeNotNull(project_id)), '') AS `project`,
       |       JSONExtractString(`address`, 'district') AS `district`,
       |       JSONExtractString(`address`, 'city') AS `city`,
       |       request_type,
       |       CASE listing_type WHEN 'REVER' THEN 'https://my.rever.vn/listing/update'
       |       ELSE 'https://my.rever.vn/mls/detail'
       |       END as link_nha_dat
       |FROM staff_request_listing_info_1
       |WHERE 1 = 1
       |  AND `timestamp` >= ${startYear}
       |  AND `timestamp` <= ${endYear}
       |ORDER BY `timestamp` DESC
       |""".stripMargin
  }
}
