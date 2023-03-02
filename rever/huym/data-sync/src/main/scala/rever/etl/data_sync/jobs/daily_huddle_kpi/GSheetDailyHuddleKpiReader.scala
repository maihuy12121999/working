package rever.etl.data_sync.jobs.daily_huddle_kpi

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import rever.etl.data_sync.util.Utils
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config
import rever.rspark.connector.gsheet.domain.GSheetOptions

import java.util.Base64

class GSheetDailyHuddleKpiReader extends SourceReader {

  override def read(s: String, config: Config): Dataset[Row] = {
    val serviceAccountKey = new String(
      Base64.getDecoder.decode(config.get("gsheet_huddle_kpi_service_account_key")),
      "UTF-8"
    )
    val spreadSheetId = config.get("gsheet_huddle_kpi_spreadsheet_id")
    val sheetName = config.get("gsheet_huddle_kpi_sheet_name")
    val dataRange = config.get("gsheet_huddle_kpi_data_range")
    val withHeader = config.getBoolean("gsheet_huddle_kpi_with_header", true)
    val df = SparkSession.active.read
      .format("rever.rspark.connector.gsheet.GSheetProvider")
      .option(GSheetOptions.CREDENTIAL, serviceAccountKey)
      .option(GSheetOptions.DATA_RANGE, dataRange)
      .option(GSheetOptions.WITH_HEADER, withHeader)
      .load(s"$spreadSheetId/$sheetName")
      .select(
        col("Interval Type").as(HuddleKpiHelper.INTERVAL_TYPE),
        col("Type").as(HuddleKpiHelper.TYPE),
        col("Dimension").as(HuddleKpiHelper.DIMENSION),
        col("Time").as(HuddleKpiHelper.TIME),
        col("Revenue").as(HuddleKpiHelper.REVENUE),
        col("Connected Call").as(HuddleKpiHelper.CONNECTED_CALL),
        col("Appointment").as(HuddleKpiHelper.APPOINTMENT),
        col("Active Listing").as(HuddleKpiHelper.ACTIVE_LISTING),
        col("Active Inquiry").as(HuddleKpiHelper.ACTIVE_INQUIRY),
        col("Booked Transaction").as(HuddleKpiHelper.BOOKED_TRANSACTION),
        col("Total Agent").as(HuddleKpiHelper.TOTAL_AGENT),
        col("Avg Revenue Per Agent").as(HuddleKpiHelper.AVG_REVENUE_PER_AGENT)
      )

    normalizeDf(df)

  }

  private def normalizeDf(df: DataFrame): DataFrame = {
    df
      .withColumn(HuddleKpiHelper.TIME, Utils.normalizedDateUdf(col(HuddleKpiHelper.TIME)))
      .withColumn(HuddleKpiHelper.REVENUE, col(HuddleKpiHelper.REVENUE).cast(DoubleType))
      .withColumn(HuddleKpiHelper.CONNECTED_CALL, col(HuddleKpiHelper.CONNECTED_CALL).cast(LongType))
      .withColumn(HuddleKpiHelper.APPOINTMENT, col(HuddleKpiHelper.APPOINTMENT).cast(LongType))
      .withColumn(HuddleKpiHelper.ACTIVE_LISTING, col(HuddleKpiHelper.ACTIVE_LISTING).cast(LongType))
      .withColumn(HuddleKpiHelper.ACTIVE_INQUIRY, col(HuddleKpiHelper.ACTIVE_INQUIRY).cast(LongType))
      .withColumn(HuddleKpiHelper.ACTIVE_INQUIRY, col(HuddleKpiHelper.ACTIVE_INQUIRY).cast(LongType))
      .withColumn(HuddleKpiHelper.BOOKED_TRANSACTION, col(HuddleKpiHelper.BOOKED_TRANSACTION).cast(LongType))
      .withColumn(HuddleKpiHelper.TOTAL_AGENT, col(HuddleKpiHelper.TOTAL_AGENT).cast(LongType))
      .withColumn(HuddleKpiHelper.AVG_REVENUE_PER_AGENT, col(HuddleKpiHelper.AVG_REVENUE_PER_AGENT).cast(DoubleType))
      .withColumn(HuddleKpiHelper.LOG_TIME, lit(System.currentTimeMillis()))
  }
}
