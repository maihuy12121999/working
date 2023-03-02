package rever.etl.data_sync.jobs.finance

import org.scalatest.Matchers.assertResult
import rever.etl.data_sync.client.GSheetClient
import rever.etl.data_sync.domain.finance._
import rever.etl.data_sync.jobs.finance.ActualOrBudgetReader.toActualTotalBURecord
import rever.etl.data_sync.util.FinanceGSheetUtils
import rever.etl.rsparkflow.utils.TimestampUtils

import scala.collection.JavaConverters.asScalaBufferConverter

trait BuActualReader extends BaseGSheetReader[BURecord] {
  def read(spreadsheetId: String, sheet: String): Seq[BURecord]
}

case class BuActualReaderImpl(
    financeSheetId: String,
    client: GSheetClient,
    dataRange: String,
    actualReader: ActualOrBudgetReader,
    fieldMapping: Map[String, Int]
) extends BuActualReader {

  override def read(spreadsheetId: String, sheet: String): Seq[BURecord] = {

    val totalReportMap: Map[String, BrokerageRecord] = actualReader
      .read(financeSheetId, "Actual")
      .head
      .brokerages
      .filter(_.dimension.exists(_.equals("ALL")))
      .map(r => {
        s"${r.periodType.getOrElse("")}_${r.periodValue.getOrElse("")}" -> r
      })
      .toMap

    val sheetDataTable = client
      .getSheetValue(
        spreadsheetId,
        range = s"${sheet}!${dataRange}",
        isRow = false,
        renderOption = "UNFORMATTED_VALUE"
      )

    val records = sheetDataTable.flatMap(row => {
      val monthInTime: Long = row.headOption.map(_.toString).map(TimestampUtils.parseMillsFromString(_, "MMM-yy")).get
      val primaryRow = row.slice(5, 14)
      val secondaryRow = row.slice(16, 25)
      val totalRow = row.slice(28, 32)

      val totalReport = totalReportMap.get(s"MONTH_${TimestampUtils.asMonth(monthInTime)}")

      Seq(
        toActualBURecord(monthInTime, "PRIMARY", primaryRow),
        toActualBURecord(monthInTime, "SECONDARY", secondaryRow),
        toActualTotalBURecord(monthInTime, totalRow, totalReport)
      )
    })
    testData(records)
    records
  }

  def toActualBURecord(
      monthInTime: Long,
      dimension: String,
      sheetRowValues: Seq[AnyRef]
  ): BURecord = {
    val transactionIdx = fieldMapping(BURecord.NUM_OF_TRANSACTIONS)
    val transactionValueIdx = fieldMapping(BURecord.GROSS_TRANSACTION_VALUE)
    val revenueIdx = fieldMapping(BURecord.REVENUE)
    val commissionIdx = fieldMapping(BURecord.COMMISSION)
    val cosIdx = fieldMapping(BURecord.SELLING)
    val marketingIdx = fieldMapping(BURecord.MARKETING)
    val personnelIdx = fieldMapping(BURecord.PERSONNEL)
    val officeAndOpsIdx = fieldMapping(BURecord.OFFICE_OPERATION)
    val profitAndLossIdx = fieldMapping(BURecord.PROFIT_AND_LOSS)
    // Get data
    val numOfTrans: Int =
      Math.round((sheetRowValues lift transactionIdx).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)).toInt
    val grossTransactionValue =
      (sheetRowValues lift transactionValueIdx).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val revenue = (sheetRowValues lift revenueIdx).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val commission = (sheetRowValues lift commissionIdx).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val grossProfit = revenue - commission
    val selling = (sheetRowValues lift cosIdx).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val marketing = (sheetRowValues lift marketingIdx).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val sellingPerRevenue = if (revenue != 0) selling / revenue else 0.0
    val marketingPerRevenue = if (revenue != 0) marketing / revenue else 0.0

    val personnel = (sheetRowValues lift personnelIdx).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val rental = 0.0
    val officeOperation = (sheetRowValues lift officeAndOpsIdx).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val personnelPerRevenue = if (revenue != 0) personnel / revenue else 0.0
    val officeOperationPerRevenue = if (revenue != 0) officeOperation / revenue else 0.0

    val profitAndLoss = (sheetRowValues lift profitAndLossIdx).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val profitAndLossPerRevenue = if (revenue != 0) profitAndLoss / revenue else 0.0
    BURecord(
      kind = Some("actual"),
      dimension = Some(dimension),
      periodType = Some("MONTH"),
      periodValue = Some(TimestampUtils.asMonth(monthInTime)),
      numOfTransactions = Some(numOfTrans),
      grossTransactionValue = Some(grossTransactionValue),
      revenue = Some(revenue),
      commission = Some(commission),
      grossProfit = Some(grossProfit),
      selling = Some(selling),
      marketing = Some(marketing),
      cosPerRevenue = Some(sellingPerRevenue),
      marketingPerRevenue = Some(marketingPerRevenue),
      personnel = Some(personnel),
      rental = Some(rental),
      officeOperation = Some(officeOperation),
      personnelPerRevenue = Some(personnelPerRevenue),
      rentalPerRevenue = Some(0.0),
      officeOperationPerRevenue = Some(officeOperationPerRevenue),
      profitAndLoss = Some(profitAndLoss),
      profitAndLossPerRevenue = Some(profitAndLossPerRevenue),
      time = Some(monthInTime),
      logTime = Some(System.currentTimeMillis())
    )

  }

  /** This method is used to validate data is correct
    */
  private def testData(buRecords: Seq[BURecord]): Unit = {

    val pRecord = buRecords
      .filter(_.dimension.exists(_.equals("PRIMARY")))
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("01/2022"))
      .head

    assertResult(pRecord.numOfTransactions.getOrElse(0), "Wrong number of transactions")(58)
    assertResult(pRecord.grossTransactionValue.getOrElse(0.0) , "Wrong gross transaction value")(652391)
    assertResult(pRecord.revenue.getOrElse(0.0) , "Wrong revenue")(10733)
    assertResult(pRecord.commission.getOrElse(0.0) , "Wrong COS - commission")(8257.0)
    assertResult(pRecord.selling.getOrElse(0.0) , "Wrong Selling cost")(561)
    assertResult(pRecord.marketing.getOrElse(0.0), "Wrong Marketing")(209)
    assertResult(pRecord.personnel.getOrElse(0.0) , "Wrong Personel")(1107)
    assertResult(pRecord.officeOperation.getOrElse(0.0), "Wrong Office & Operation")(317)
    assertResult(pRecord.profitAndLoss.getOrElse(0.0), "Wrong P&L")(282.0)

    val sRecord = buRecords
      .filter(_.dimension.exists(_.equals("SECONDARY")))
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("01/2022"))
      .head
    assertResult(sRecord.numOfTransactions.getOrElse(0), "Wrong number of transactions")( 51)
    assertResult(sRecord.grossTransactionValue.getOrElse(0.0), "Wrong gross transaction value")(220157.2)
    assertResult(sRecord.revenue.getOrElse(0.0), "Wrong revenue")(872)
    assertResult(sRecord.commission.getOrElse(0.0) , "Wrong COS - commission")(461)
    assertResult(sRecord.selling.getOrElse(0.0), "Wrong Selling cost")(133)
    assertResult(sRecord.marketing.getOrElse(0.0), "Wrong Marketing")(142)
    assertResult(sRecord.personnel.getOrElse(0.0) , "Wrong Personel")(907)
    assertResult(sRecord.officeOperation.getOrElse(0.0) , "Wrong Office & Operation")(278)
    assertResult(sRecord.profitAndLoss.getOrElse(0.0) , "Wrong P&L")(-1049)

    val totalRecord = buRecords
      .filter(_.dimension.exists(_.equals("ALL")))
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("01/2022"))
      .head
    assertResult(totalRecord.numOfTransactions.getOrElse(0) , "Wrong number of transactions")(109)

    //TODO: Enable this
//    assert(totalRecord.grossTransactionValue.getOrElse(0.0).toInt == 872548, "Wrong gross transaction value")
    assertResult(totalRecord.revenue.getOrElse(0.0).toInt, "Wrong revenue")(11605)
    assertResult(totalRecord.commission.getOrElse(0.0).toInt, "Wrong COS - commission")(8718)
    assertResult(totalRecord.selling.getOrElse(0.0), "Wrong Selling cost")(694)
    assertResult(totalRecord.marketing.getOrElse(0.0), "Wrong Marketing")(959)
    assertResult(totalRecord.personnel.getOrElse(0.0), "Wrong Personel")(5294)
    assertResult(totalRecord.officeOperation.getOrElse(0.0), "Wrong Office & Operation")(1101.0)
    assertResult(totalRecord.profitAndLoss.getOrElse(0.0).toInt, "Wrong P&L")(-4873)

  }

}
