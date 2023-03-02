package rever.etl.data_sync.jobs.finance

import org.scalatest.Matchers.assertResult
import rever.etl.data_sync.client.GSheetClient
import rever.etl.data_sync.domain.finance._
import rever.etl.data_sync.util.FinanceGSheetUtils
import rever.etl.rsparkflow.utils.TimestampUtils

import scala.collection.mutable.ListBuffer

trait BoCostReader extends BaseGSheetReader[BoCostRecord] {
  def read(spreadsheetId: String, sheet: String): Seq[BoCostRecord]
}

case class BoCostReaderImpl(
    client: GSheetClient,
    dataRange: String,
    categoryMapping: Map[String, String],
    costTypeMapping: Map[String, String]
) extends BoCostReader {

  override def read(spreadsheetId: String, sheet: String): Seq[BoCostRecord] = {

    val records = ListBuffer.empty[BoCostRecord]

    client.foreachRows(spreadsheetId, s"${sheet}!${dataRange}", 2, 10) { row =>
      val dataRow = row.map(_.asInstanceOf[AnyRef])

      records.append(toBoCost("actual", dataRow))
    }

    testData(records)

    records
  }

  private def toBoCost(kind: String, sheetRowValues: Seq[AnyRef]): BoCostRecord = {
    val monthStr = sheetRowValues.headOption
      .map(_.toString.trim)
      .getOrElse("")
      .replaceAll("\\(.+\\)", "")
      .trim
    val monthInTime: Long = TimestampUtils.parseMillsFromString(monthStr, "MMM-yy")

    val category = categoryMapping.getOrElse(sheetRowValues(1).toString, "unknown")
    val costType = costTypeMapping.getOrElse(sheetRowValues(2).toString, "unknown")
    val amount = (sheetRowValues lift 3).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)

    BoCostRecord(
      kind = Some(kind),
      periodType = Some("MONTH"),
      periodValue = Some(TimestampUtils.asMonth(monthInTime)),
      category = Some(category),
      costType = Some(costType),
      amount = Some(amount),
      time = Some(monthInTime),
      logTime = Some(System.currentTimeMillis())
    )

  }

  /** This method is used to validate data is correct
    */
  private def testData(records: Seq[BoCostRecord]): Unit = {

    val record1 = records
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("01/2021"))
      .filter(_.category.getOrElse().equals("other_benefit"))
      .filter(_.costType.getOrElse().equals("recruitment_fee"))
      .head
    assertResult(record1.amount.getOrElse(0.0).toInt, "Wrong Recruitment fee")(72)

    val record2 = records
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("01/2021"))
      .filter(_.category.getOrElse().equals("other_benefit"))
      .filter(_.costType.getOrElse().equals("training_cost"))
      .head
    assertResult(record2.amount.getOrElse(0.0).toInt, "Wrong Training cost")(5)

    val record3 = records
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("01/2021"))
      .filter(_.category.getOrElse().equals("other_benefit"))
      .filter(_.costType.getOrElse().equals("automobile_expense"))
      .head
    assertResult(record3.amount.getOrElse(0.0).toInt, "Wrong Automobile Expense")(43)

    val record4 = records
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("01/2021"))
      .filter(_.category.getOrElse().equals("other_benefit"))
      .filter(_.costType.getOrElse().equals("others"))
      .head
    assertResult(record4.amount.getOrElse(0.0).toInt, "Wrong Le tet su kien ....")(130)

    val record5 = records
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("01/2021"))
      .filter(_.category.getOrElse().equals("salary_and_bonus"))
      .filter(_.costType.getOrElse().equals("tech"))
      .head
    assertResult(record5.amount.getOrElse(0.0).toInt, "Wrong Tech")( 650)

    val record6 = records
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("01/2021"))
      .filter(_.category.getOrElse().equals("salary_and_bonus"))
      .filter(_.costType.getOrElse().equals("other_department"))
      .head
    assertResult(record6.amount.getOrElse(0.0).toInt, "Wrong Other Department")(1383)
  }

}
