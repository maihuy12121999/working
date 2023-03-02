package restore_oppo_historical.reader

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import restore_oppo_historical.RestoreOppoHistoricalHelper
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config
import rever.rspark.connector.gsheet.domain.GSheetOptions

import java.util.Base64

class GSheetMissingOppoHistoryReader extends SourceReader {
  override def read(s: String, config: Config): Dataset[Row] = {

    val serviceAccountKey = new String(
      Base64.getDecoder.decode(config.get("gsheet_missing_oppo_service_account_key")),
      "UTF-8"
    )

    val spreadSheetId = config.get("gsheet_missing_oppo_spreadsheet_id")
    val sheetName = config.get("gsheet_missing_oppo_sheet_name")
    val dataRange = config.get("gsheet_missing_oppo_data_range")
    val withHeader = config.getBoolean("gsheet_missing_oppo_with_header", true)

    val df = SparkSession.active.read
      .format("rever.rspark.connector.gsheet.GSheetProvider")
      .option(GSheetOptions.CREDENTIAL, serviceAccountKey)
      .option(GSheetOptions.DATA_RANGE, dataRange)
      .option(GSheetOptions.WITH_HEADER, withHeader)
      .load(s"$spreadSheetId/$sheetName")
      .withColumn(RestoreOppoHistoricalHelper.ACTION, lit("2"))
      .withColumn(RestoreOppoHistoricalHelper.OBJECT_ID, col("Oppo ID"))
      .withColumn(RestoreOppoHistoricalHelper.OLD_DATA, col("Old data"))
      .withColumn(RestoreOppoHistoricalHelper.DATA, col("New data"))
      .withColumn(RestoreOppoHistoricalHelper.SOURCE, lit("data_restore_service"))
      .withColumn(RestoreOppoHistoricalHelper.PERFORMER, lit("restore_missing_oppo_202211"))
      .withColumn(RestoreOppoHistoricalHelper.TIMESTAMP, col("Timestamp").cast(LongType))
      .withColumn(RestoreOppoHistoricalHelper.TYPE, lit("missing_oppo_history"))
      .select(
        col(RestoreOppoHistoricalHelper.ACTION),
        col(RestoreOppoHistoricalHelper.OBJECT_ID),
        col(RestoreOppoHistoricalHelper.OLD_DATA),
        col(RestoreOppoHistoricalHelper.DATA),
        col(RestoreOppoHistoricalHelper.SOURCE),
        col(RestoreOppoHistoricalHelper.PERFORMER),
        col(RestoreOppoHistoricalHelper.TIMESTAMP),
        col(RestoreOppoHistoricalHelper.TYPE)
      )

    df
  }
}
