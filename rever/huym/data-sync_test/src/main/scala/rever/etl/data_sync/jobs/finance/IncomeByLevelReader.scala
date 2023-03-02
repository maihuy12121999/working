package rever.etl.data_sync.jobs.finance

import org.scalatest.Matchers.assertResult
import rever.etl.data_sync.client.GSheetClient
import rever.etl.data_sync.domain.finance._
import rever.etl.data_sync.util.FinanceGSheetUtils
import rever.etl.rsparkflow.utils.TimestampUtils

import scala.collection.mutable.ListBuffer

trait IncomeByLevelReader extends BaseGSheetReader[IncomeByLevel] {
  def read(spreadsheetId: String, sheet: String): Seq[IncomeByLevel]
}

case class IncomeByLevelReaderImpl(
    client: GSheetClient,
    dataRange: String,
    buMapping: Map[String, String],
    jobLevelMapping: Map[String, String]
) extends IncomeByLevelReader {

  override def read(spreadsheetId: String, sheet: String): Seq[IncomeByLevel] = {

    val records = ListBuffer.empty[IncomeByLevel]

    client.foreachRows(spreadsheetId, s"${sheet}!${dataRange}", 2, 10) { row =>
      val dataRow = row.map(_.asInstanceOf[AnyRef])
      records.append(toIncomeByLevel(dataRow))
    }

    testData(records)

    records.filterNot(record => {
      record.amount.getOrElse(0.0) == 0 && record.headCount.getOrElse(0) == 0
    })
  }

  private def toIncomeByLevel(sheetRowValues: Seq[AnyRef]): IncomeByLevel = {
    val monthStr = sheetRowValues.headOption
      .map(_.toString.trim)
      .getOrElse("")
      .replaceAll("\\(.+\\)", "")
      .trim
    val monthInTime: Long = TimestampUtils.parseMillsFromString(monthStr, "MMM-yy")

    val businessUnit = buMapping.getOrElse(sheetRowValues(1).toString.trim.toLowerCase, "UNKNOWN")
    val level = jobLevelMapping.getOrElse(sheetRowValues(2).toString.trim.toLowerCase, "UNKNOWN")
    val amount = (sheetRowValues lift 3).map(FinanceGSheetUtils.asDouble).getOrElse(0.0).round.toDouble
    val headCount = (sheetRowValues lift 4).map(FinanceGSheetUtils.asDouble).getOrElse(0.0).round.toInt

    IncomeByLevel(
      periodType = Some("MONTH"),
      periodValue = Some(TimestampUtils.asMonth(monthInTime)),
      businessUnit = Some(businessUnit),
      level = Some(level),
      amount = Some(amount),
      headCount = Some(headCount),
      time = Some(monthInTime),
      logTime = Some(System.currentTimeMillis())
    )

  }

  /** This method is used to validate data is correct
    */
  private def testData(records: Seq[IncomeByLevel]): Unit = {

    val record1 = records
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("01/2022"))
      .filter(_.businessUnit.getOrElse().equals("PRIMARY"))
      .filter(_.level.getOrElse().equals("sd"))
      .head
    assertResult( 185, "Wrong Amount")(record1.amount.getOrElse(0.0).toInt)
    assertResult( 5, "Wrong headcount")(record1.headCount.getOrElse(0))

  }

}
