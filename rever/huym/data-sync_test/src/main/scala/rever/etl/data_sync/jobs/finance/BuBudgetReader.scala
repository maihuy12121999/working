package rever.etl.data_sync.jobs.finance

import org.scalatest.Matchers.assertResult
import rever.etl.data_sync.client.GSheetClient
import rever.etl.data_sync.domain.finance._
import rever.etl.data_sync.util.FinanceGSheetUtils
import rever.etl.rsparkflow.utils.TimestampUtils

import scala.collection.JavaConverters.asScalaBufferConverter

trait BuBudgetReader extends BaseGSheetReader[BURecord] {
  def read(spreadsheetId: String, sheet: String): Seq[BURecord]
}

case class BuBudgetReaderImpl(
    client: GSheetClient,
    dataRange: String,
    fieldMapping: Map[String, Int]
) extends BuBudgetReader {

  override def read(spreadsheetId: String, sheet: String): Seq[BURecord] = {

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
      .filterNot(_.isEmpty)

    val records = sheetDataTable.flatMap(row => {
      val monthInTime: Long = row.headOption.map(_.toString).map(TimestampUtils.parseMillsFromString(_, "MMM-yy")).get
      val secondaryRow = row.slice(2, 12)
      val primaryRow = row.slice(14, 24)
      val partnerRow = row.slice(26, 36)
      val totalRow = row.slice(38, 48)
      Seq(
        toBudgetBURecord(monthInTime, "PRIMARY", primaryRow),
        toBudgetBURecord(monthInTime, "SECONDARY", secondaryRow),
        toBudgetBURecord(monthInTime, "PARTNER", partnerRow),
        toBudgetBURecord(monthInTime, "ALL", totalRow)
      )
    })
    testData(records)
    records
  }

  private def toBudgetBURecord(monthInTime: Long, dimension: String, sheetRowValues: Seq[AnyRef]): BURecord = {
    val transactionIdx = fieldMapping(BURecord.NUM_OF_TRANSACTIONS)
    val transactionValueIdx = fieldMapping(BURecord.GROSS_TRANSACTION_VALUE)
    val revenueIdx = fieldMapping(BURecord.REVENUE)
    val commissionIdx = fieldMapping(BURecord.COMMISSION)
    val sellingIdx = fieldMapping(BURecord.SELLING)
    val marketingIdx = fieldMapping(BURecord.MARKETING)
    val personnelIdx = fieldMapping(BURecord.PERSONNEL)
    val rentalIdx = fieldMapping(BURecord.RENTAL)
    val officeAndOpsIdx = fieldMapping(BURecord.OFFICE_OPERATION)
    val profitAndLossIdx = fieldMapping(BURecord.PROFIT_AND_LOSS)

    val numTransactions: Int =
      Math.round((sheetRowValues lift transactionIdx).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)).toInt
    val grossTransactionValue =
      (sheetRowValues lift transactionValueIdx).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val revenue = (sheetRowValues lift revenueIdx).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val commission = (sheetRowValues lift commissionIdx).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val grossProfit = revenue - commission
    val selling = (sheetRowValues lift sellingIdx).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val marketing = (sheetRowValues lift marketingIdx).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val cosPerRevenue = if (revenue != 0) selling / revenue else 0.0
    val marketingPerRevenue = if (revenue != 0) marketing / revenue else 0.0

    val personnel = (sheetRowValues lift personnelIdx).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val rental = (sheetRowValues lift rentalIdx).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val officeOperation = (sheetRowValues lift officeAndOpsIdx).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val personnelPerRevenue = if (revenue != 0) personnel / revenue else 0.0
    val rentalPerRevenue = if (revenue != 0) rental / revenue else 0.0
    val officeOperationPerRevenue = if (revenue != 0) officeOperation / revenue else 0.0

    val profitAndLoss = (sheetRowValues lift profitAndLossIdx).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val profitAndLossPerRevenue = if (revenue != 0) profitAndLoss / revenue else 0.0

    BURecord(
      kind = Some("budget"),
      dimension = Some(dimension),
      periodType = Some("MONTH"),
      periodValue = Some(TimestampUtils.asMonth(monthInTime)),
      numOfTransactions = Some(numTransactions),
      grossTransactionValue = Some(grossTransactionValue),
      revenue = Some(revenue),
      commission = Some(commission),
      grossProfit = Some(grossProfit),
      selling = Some(selling),
      marketing = Some(marketing),
      cosPerRevenue = Some(cosPerRevenue),
      marketingPerRevenue = Some(marketingPerRevenue),
      personnel = Some(personnel),
      rental = Some(rental),
      officeOperation = Some(officeOperation),
      personnelPerRevenue = Some(personnelPerRevenue),
      rentalPerRevenue = Some(rentalPerRevenue),
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
      .filter(_.periodValue.getOrElse().equals("12/2021"))
      .head

    assertResult(56, "Wrong number of transactions")(
      pRecord.numOfTransactions.getOrElse(0)
    )
    assertResult(260450, "Wrong gross transaction value")(pRecord.grossTransactionValue.getOrElse(0.0))
    assertResult(7813, "Wrong revenue")(pRecord.revenue.getOrElse(0.0))
    assertResult(5321, "Wrong COS - commission")(pRecord.commission.getOrElse(0.0))
    assertResult(314, "Wrong Selling cost")(pRecord.selling.getOrElse(0.0))
    assertResult(305, "Wrong Marketing")(pRecord.marketing.getOrElse(0.0))
    assertResult(861, "Wrong Personel")(pRecord.personnel.getOrElse(0.0))
    assertResult(235, "Wrong Office & Operation")(pRecord.officeOperation.getOrElse(0.0).toInt)
    assertResult(394, "Wrong Rental cost")(pRecord.rental.getOrElse(0.0))
    assertResult(383, "Wrong P&L")(pRecord.profitAndLoss.getOrElse(0.0))

    val sRecord = buRecords
      .filter(_.dimension.exists(_.equals("SECONDARY")))
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("12/2021"))
      .head
    assertResult(143, "Wrong number of transactions")(sRecord.numOfTransactions.getOrElse(0))
    assertResult(246241, "Wrong gross transaction value")(sRecord.grossTransactionValue.getOrElse(0.0).toInt)
    assertResult(2462, "Wrong revenue")(sRecord.revenue.getOrElse(0.0))
    assertResult(1539, "Wrong COS - commission")(sRecord.commission.getOrElse(0.0))
    assertResult(288, "Wrong Selling cost")(sRecord.selling.getOrElse(0.0))
    assertResult(98, "Wrong Marketing")(sRecord.marketing.getOrElse(0.0))
    assertResult(737, "Wrong Personel")(sRecord.personnel.getOrElse(0.0))
    assertResult(262, "Wrong Office & Operation")(sRecord.officeOperation.getOrElse(0.0))
    assertResult(298, "Wrong Rental cost")(sRecord.rental.getOrElse(0.0))
    assertResult(-760, "Wrong P&L")(sRecord.profitAndLoss.getOrElse(0.0))

    val totalRecord = buRecords
      .filter(_.dimension.exists(_.equals("ALL")))
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("12/2021"))
      .head
    assertResult(56, "Wrong number of transactions")(totalRecord.numOfTransactions.getOrElse(0))
    assertResult(260450, "Wrong gross transaction value")(totalRecord.grossTransactionValue.getOrElse(0.0).toInt)
    assertResult(7813, "Wrong revenue")(totalRecord.revenue.getOrElse(0.0).toInt)
    assertResult(5321, "Wrong COS - commission")(totalRecord.commission.getOrElse(0.0).toInt)
    assertResult(314, "Wrong Selling cost")(totalRecord.selling.getOrElse(0.0))
    assertResult(305, "Wrong Marketing")(totalRecord.marketing.getOrElse(0.0))
    assertResult(861, "Wrong Personel")(totalRecord.personnel.getOrElse(0.0))
    assertResult(394, "Wrong Rental cost")(totalRecord.rental.getOrElse(0.0))
    assertResult(235, "Wrong Office & Operation")(totalRecord.officeOperation.getOrElse(0.0))
    assertResult(383, "Wrong P&L")(totalRecord.profitAndLoss.getOrElse(0.0).toInt)

  }

}
