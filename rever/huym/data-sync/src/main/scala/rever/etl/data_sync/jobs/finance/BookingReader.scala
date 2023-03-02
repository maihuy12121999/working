package rever.etl.data_sync.jobs.finance

import org.scalatest.Matchers.assertResult
import rever.etl.data_sync.client.GSheetClient
import rever.etl.data_sync.domain.finance._
import rever.etl.data_sync.util.FinanceGSheetUtils
import rever.etl.rsparkflow.utils.TimestampUtils

import scala.collection.mutable.ListBuffer

trait BookingReader extends BaseGSheetReader[BookingRecord] {
  def read(spreadsheetId: String, sheet: String): Seq[BookingRecord]
}

case class BookingReaderImpl(client: GSheetClient, dataRange: String) extends BookingReader {

  override def read(spreadsheetId: String, sheet: String): Seq[BookingRecord] = {
    val bookingRecords = ListBuffer.empty[BookingRecord]

    client.foreachRows(spreadsheetId, s"${sheet}!${dataRange}", 3, 10) { row =>
      val monthInTime: Long = row.headOption.map(_.toString).map(TimestampUtils.parseMillsFromString(_, "MMM-yy")).get
      val dataRow = row.slice(1, 12).map(_.asInstanceOf[AnyRef])

      bookingRecords.appendAll(toBookingRecords(monthInTime, dataRow))
    }

    testData(bookingRecords)
    bookingRecords
  }

  private def toBookingRecords(monthInTime: Long, sheetRowValues: Seq[AnyRef]): Seq[BookingRecord] = {
    val totalTransactions: Int =
      Math.round((sheetRowValues lift 0).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)).toInt
    val totalRevenue = (sheetRowValues lift 1).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val totalPL = (sheetRowValues lift 2).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)

    //Primary
    val primaryTransactions: Int =
      Math.round((sheetRowValues lift 4).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)).toInt
    val primaryRevenue = (sheetRowValues lift 5).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val primaryPL = (sheetRowValues lift 6).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)

    //Secondary
    val secondaryTransactions: Int =
      Math.round((sheetRowValues lift 8).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)).toInt
    val secondaryRevenue = (sheetRowValues lift 9).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val secondaryPL = (sheetRowValues lift 10).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)

    Seq(

      BookingRecord(
        periodType = Some("MONTH"),
        periodValue = Some(TimestampUtils.asMonth(monthInTime)),
        businessUnit = Some("ALL"),
        numOfTransactions = Some(totalTransactions),
        revenue = Some(totalRevenue),
        profitAndLoss = Some(totalPL),
        time = Some(monthInTime),
        logTime = Some(System.currentTimeMillis())
      ),
      BookingRecord(
        periodType = Some("MONTH"),
        periodValue = Some(TimestampUtils.asMonth(monthInTime)),
        businessUnit = Some("PRIMARY"),
        numOfTransactions = Some(primaryTransactions),
        revenue = Some(primaryRevenue),
        profitAndLoss = Some(primaryPL),
        time = Some(monthInTime),
        logTime = Some(System.currentTimeMillis())
      ),
      BookingRecord(
        periodType = Some("MONTH"),
        periodValue = Some(TimestampUtils.asMonth(monthInTime)),
        businessUnit = Some("SECONDARY"),
        numOfTransactions = Some(secondaryTransactions),
        revenue = Some(secondaryRevenue),
        profitAndLoss = Some(secondaryPL),
        time = Some(monthInTime),
        logTime = Some(System.currentTimeMillis())
      )
    )

  }

  /** This method is used to validate data is correct
    */
  private def testData(bookingRecords: Seq[BookingRecord]): Unit = {

    val pRecord = bookingRecords
      .filter(_.businessUnit.exists(_.equals("PRIMARY")))
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("01/2022"))
      .head

    assertResult(pRecord.numOfTransactions.getOrElse(0) , "Wrong number of transactions")(12)
    assertResult(pRecord.revenue.getOrElse(0.0) , "Wrong revenue")(1176)
    assertResult(pRecord.profitAndLoss.getOrElse(0.0) , "Wrong P&L")(-1925.0)

    val sRecord = bookingRecords
      .filter(_.businessUnit.exists(_.equals("SECONDARY")))
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("01/2022"))
      .head
    assertResult(sRecord.numOfTransactions.getOrElse(0), "Wrong number of transactions")(50)
    assertResult(sRecord.revenue.getOrElse(0.0).toInt , "Wrong revenue")(592)
    assertResult(sRecord.profitAndLoss.getOrElse(0.0), "Wrong P&L")(-1211)

    val totalRecord = bookingRecords
      .filter(_.businessUnit.exists(_.equals("ALL")))
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("01/2022"))
      .head
    assertResult(totalRecord.numOfTransactions.getOrElse(0), "Wrong number of transactions")(62)
    assertResult(totalRecord.revenue.getOrElse(0.0).toInt , "Wrong revenue")(1768)
    assertResult(totalRecord.profitAndLoss.getOrElse(0.0).toInt, "Wrong P&L")(-7241)

  }

}
