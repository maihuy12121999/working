package rever.etl.data_sync.jobs.finance

import rever.etl.data_sync.client.GSheetClient
import rever.etl.data_sync.domain.finance._
import rever.etl.data_sync.util.FinanceGSheetUtils
import rever.etl.rsparkflow.utils.TimestampUtils

import scala.collection.JavaConverters.asScalaBufferConverter

trait CostReader extends BaseGSheetReader[CostRecord] {
  def read(spreadsheetId: String, sheet: String): Seq[CostRecord]
}

case class CostReaderImpl(client: GSheetClient, dataRange: String) extends CostReader {

  override def read(spreadsheetId: String, sheet: String): Seq[CostRecord] = {

    val sheetDataTable = client
      .getSheetValue(
        spreadsheetId,
        range = s"${sheet}!${dataRange}",
        isRow = false,
        renderOption = "UNFORMATTED_VALUE"
      )
      .getValues
      .asScala
      .map(_.asScala.toSeq)
      .toSeq

    sheetDataTable.map(row => {
      val monthStr = row.headOption.map(_.toString.trim).getOrElse("")
      val monthInTime: Long = TimestampUtils.parseMillsFromString(
        monthStr.replaceAll("\\(.+\\)", "").trim,
        "MMM-yy"
      )
      val kind = if (monthStr.contains("Bud")) "budget" else "actual"
      toCostRecord(kind, monthInTime, row)
    })
  }

  private def toCostRecord(kind: String, monthInTime: Long, sheetRowValues: Seq[AnyRef]): CostRecord = {
    val salePrimary = (sheetRowValues lift 2).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val saleSecondary = (sheetRowValues lift 3).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val techPersonnel = (sheetRowValues lift 4).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val otherDeptPersonnel = (sheetRowValues lift 5).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val recruitmentPersonnel = (sheetRowValues lift 6).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val rentalTransactionCenterOffice = (sheetRowValues lift 9).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val rentalHeadOffice = (sheetRowValues lift 10).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val officeOperation = (sheetRowValues lift 11).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val depreciationOffice = (sheetRowValues lift 12).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val toolAndServer = (sheetRowValues lift 14).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val otherGainLoss = (sheetRowValues lift 15).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val total = (sheetRowValues lift 17).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)

    CostRecord(
      kind = Some(kind),
      periodType = Some("MONTH"),
      periodValue = Some(TimestampUtils.asMonth(monthInTime)),
      salePrimaryPersonnel = Some(salePrimary),
      saleSecondaryPersonnel = Some(saleSecondary),
      techPersonnel = Some(techPersonnel),
      otherDeptPersonnel = Some(otherDeptPersonnel),
      recruitmentPersonnel = Some(recruitmentPersonnel),
      rentalTransactionCenterOffice = Some(rentalTransactionCenterOffice),
      rentalHeadOffice = Some(rentalHeadOffice),
      operationOffice = Some(officeOperation),
      depreciationOffice = Some(depreciationOffice),
      toolAndServer = Some(toolAndServer),
      otherGainLoss = Some(otherGainLoss),
      total = Some(total),
      time = Some(monthInTime),
      logTime = Some(System.currentTimeMillis())
    )

  }

}
