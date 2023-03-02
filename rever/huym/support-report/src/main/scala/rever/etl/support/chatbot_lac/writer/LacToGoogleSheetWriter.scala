package rever.etl.support.chatbot_lac.writer

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import rever.rsparkflow.spark.api.SinkWriter
import rever.rsparkflow.spark.api.configuration.Config
import rever.etl.support.client.GSheetClient

import java.util.Base64

class LacToGoogleSheetWriter extends SinkWriter {

  override def write(tableName: String, dataFrame: DataFrame, config: Config): DataFrame = {
    val exportSpreadsheetId = config.get("export_spreadsheet_id")
    val exportSheetName = config.get("export_sheet_name")
    val exportAuthServiceAccount = new String(
      Base64.getDecoder.decode(config.get("export_google_service_account_base64")),
      "UTF-8"
    )
    val maxColumnCount = config.getInt("export_sheet_column_count")
    val startEditColumnCount = config.getInt("export_sheet_start_edit_column_index")
    val endEditColumnCount = config.getInt("export_sheet_end_edit_column_index")
    val startRowIdx = config.getInt("export_start_row_index", 1)

    val exportDf = dataFrame
      .coalesce(1)
      .persist(StorageLevel.MEMORY_AND_DISK_2)

    val totalRowCount = exportDf.count()

    val client = GSheetClient.client(exportAuthServiceAccount)
    val sheetId = client.findSheetId(exportSpreadsheetId, exportSheetName)

    client.setSheetMaxRows(
      exportSpreadsheetId,
      sheetId,
      exportSheetName,
      maxColumnCount,
      startRowIdx + totalRowCount.toInt + 10
//      startRowIdx + 50
    )
    client.clearSheetRange(
      exportSpreadsheetId,
      sheetId,
      startEditColumnCount,
      endEditColumnCount,
      startRowIdx,
      startRowIdx + totalRowCount.toInt
    )
    client.beginWriteAt(startRowIdx)

    exportDf.foreachPartition((rows: Iterator[Row]) => {

      val client = GSheetClient.client(exportAuthServiceAccount)

      rows
        .grouped(400)
        .foreach(dataRows => {
          client.appendRows(exportSpreadsheetId, sheetId, dataRows)
          Thread.sleep(1500)
        })

    })

    dataFrame

  }

}
