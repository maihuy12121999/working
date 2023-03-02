package rever.etl.data_sync.jobs.finance

import org.scalatest.Matchers.assertResult
import rever.etl.data_sync.client.GSheetClient
import rever.etl.data_sync.domain.finance._
import rever.etl.rsparkflow.utils.TimestampUtils

import scala.collection.JavaConverters.asScalaBufferConverter

case class BudgetReaderImpl(client: GSheetClient, dataRange: String) extends ActualOrBudgetReader {

  val kind = "budget"

  override def read(spreadsheetId: String, sheet: String): Seq[FinanceData] = {

    val sheetDataTable: Seq[Seq[AnyRef]] = client
      .getSheetValue(
        spreadsheetId,
        range = s"${sheet}!${dataRange}",
        isRow = false,
        renderOption = "UNFORMATTED_VALUE"
      )

    val brokerages = readTotalReports(kind, sheetDataTable) ++ readPrimaryAndSecondary(kind, sheetDataTable)
    val nonBrokerages = readNonBrokerageReports(kind, sheetDataTable)
    val backOffices = readPlatformReport(kind, sheetDataTable)
    val data = FinanceData(brokerages, nonBrokerages, backOffices)
    testData(data)
    Seq(data)
  }

  def readTotalReports(kind: String, sheetDataTable: Seq[Seq[AnyRef]]): Seq[BrokerageRecord] = {
    sheetDataTable.map(row => {
      val monthInTime: Long = row.headOption.map(_.toString).map(TimestampUtils.parseMillsFromString(_, "MMM-yy")).get
      val rvFinanceRow = row.slice(61, 75)
      val record = ActualOrBudgetReader.toTotalReverRecord(kind, monthInTime, rvFinanceRow)
      record
    })
  }

  def readPrimaryAndSecondary(kind: String, sheetDataTable: Seq[Seq[AnyRef]]): Seq[BrokerageRecord] = {
    sheetDataTable.flatMap(row => {
      val monthInTime: Long = row.headOption.map(_.toString).map(TimestampUtils.parseMillsFromString(_, "MMM-yy")).get
      val primaryBrokerageRow = row.slice(2, 16)
      val secondaryBrokerageRow = row.slice(17, 31)

      val primaryRecord = ActualOrBudgetReader.toBrokerageRecord(
        kind,
        monthInTime,
        dimension = "PRIMARY",
        primaryBrokerageRow
      )
      val secondaryRecord = ActualOrBudgetReader.toBrokerageRecord(
        kind,
        monthInTime,
        dimension = "SECONDARY",
        secondaryBrokerageRow
      )

      Seq(primaryRecord, secondaryRecord)

    })
  }

  def readNonBrokerageReports(kind: String, sheetDataTable: Seq[Seq[AnyRef]]): Seq[NonBrokerageRecord] = {
    sheetDataTable.map(row => {
      val monthInTime: Long = row.headOption.map(_.toString).map(TimestampUtils.parseMillsFromString(_, "MMM-yy")).get
      val nonBrokerageRow = row.slice(32, 35)

      val record = ActualOrBudgetReader.toNonBrokerageRecord(kind, monthInTime, nonBrokerageRow)

      record
    })
  }

  def readPlatformReport(kind: String, sheetDataTable: Seq[Seq[AnyRef]]): Seq[PlatformRecord] = {
    sheetDataTable.map(row => {
      val monthInTime: Long = row.headOption.map(_.toString).map(TimestampUtils.parseMillsFromString(_, "MMM-yy")).get
      val platformRow = row.slice(51, 60)

      ActualOrBudgetReader.toPlatformRecord(kind, monthInTime, platformRow)
    })
  }

  /** This method is used to validate data is correct
    */
  private def testData(financeData: FinanceData): Unit = {

    val pRecord = financeData.brokerages
      .filter(_.dimension.exists(_.equals("PRIMARY")))
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("01/2022"))
      .head

    assertResult(56, "Wrong number of transactions")(pRecord.numOfTransactions.getOrElse(0))
    assertResult(0, "Wrong gross transaction value")(pRecord.grossTransactionValue.getOrElse(0.0))
    assertResult(10733, "Wrong revenue")(pRecord.revenue.getOrElse(0.0))
    assertResult(-8257, "Wrong COS - commission")(pRecord.cog.getOrElse(0.0))
    assertResult(2476, "Wrong Gross profit")(pRecord.grossProfit.getOrElse(0.0))
    assertResult(23, "Wrong Gross profit/Rev")((pRecord.grossProfitPerRevenue.getOrElse(0.0) * 100).toInt)
    assertResult(-771, "Wrong Selling & Marketing")(pRecord.sellingAndMarketing.getOrElse(0.0))
    assertResult(-7, "Wrong Selling & Marketing/Rev")(
      (pRecord.sellingAndMarketingPerRevenue.getOrElse(0.0) * 100).toInt
    )
    assertResult(-1424, "Wrong Personel, Office & Operation")(pRecord.personnelOfficeOperation.getOrElse(0.0))
    assertResult(
      -13,
      "Wrong Personel, Office & Operation/Rev"
    )((pRecord.personnelOfficeOperationPerRevenue.getOrElse(0.0) * 100).toInt)
    assertResult(281, "Wrong P&L")(pRecord.profitAndLoss.getOrElse(0.0))
    assertResult(2, "Wrong P&L/Rev")((pRecord.profitAndLossPerRevenue.getOrElse(0.0) * 100).toInt)

    val sRecord = financeData.brokerages
      .filter(_.dimension.exists(_.equals("SECONDARY")))
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("01/2022"))
      .head
    assertResult(47, "Wrong number of transactions")(sRecord.numOfTransactions.getOrElse(0))
    assertResult(0, "Wrong gross transaction value")(sRecord.grossTransactionValue.getOrElse(0.0))
    assertResult(872, "Wrong revenue")(sRecord.revenue.getOrElse(0.0))
    assertResult(-461, "Wrong COS - commission")(sRecord.cog.getOrElse(0.0))
    assertResult(411, "Wrong Gross profit")(sRecord.grossProfit.getOrElse(0.0))
    assertResult(47, "Wrong Gross profit/Rev")((sRecord.grossProfitPerRevenue.getOrElse(0.0) * 100).toInt)
    assertResult(-275.0, "Wrong Selling & Marketing")(sRecord.sellingAndMarketing.getOrElse(0.0))
    assertResult(-31, "Wrong Selling & Marketing/Rev")(
      (sRecord.sellingAndMarketingPerRevenue.getOrElse(0.0) * 100).toInt
    )
    assertResult(-1186, "Wrong Personel, Office & Operation")(sRecord.personnelOfficeOperation.getOrElse(0.0))
    assertResult(-136, "Wrong Personel, Office & Operation/Rev")(
      (sRecord.personnelOfficeOperationPerRevenue.getOrElse(0.0) * 100).toInt
    )
    assertResult(-1050.0, "Wrong P&L")(sRecord.profitAndLoss.getOrElse(0.0))
    assertResult(-120, "Wrong P&L/Rev")((sRecord.profitAndLossPerRevenue.getOrElse(0.0) * 100).toInt)

    val nonRecord = financeData.nonBrokerages
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("05/2021"))
      .head
    assertResult(0, "Wrong revenue")(nonRecord.revenue.getOrElse(0))
    assertResult(0, "Wrong expense")(nonRecord.expense.getOrElse(0.0))
    assertResult(0, "Wrong P&L")(nonRecord.operatingProfitAndLoss.getOrElse(0.0))

    val boRecord = financeData.backOffices
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("12/2021"))
      .head
    assertResult(-350, "Wrong tool & server")(boRecord.toolAndServer.getOrElse(0.0).toInt)
    assertResult(-308, "Wrong mkt")(boRecord.marketing.getOrElse(0.0).toInt)
    assertResult(-2995, "Wrong personel")(boRecord.personnel.getOrElse(0.0).toInt)
    assertResult(-875, "Wrong tech")(boRecord.tech.getOrElse(0.0).toInt)
    assertResult(-2120, "Wrong other dept")(boRecord.otherDept.getOrElse(0.0).toInt)
    assertResult(-595, "Wrong office operation")(boRecord.officeOperation.getOrElse(0.0).toInt)
    assertResult(0, "Wrong Interest and other gain losss")(boRecord.interestAndOtherGainLoss.getOrElse(0.0).toInt)
    assertResult(-4249, "Wrong expense")(boRecord.expense.getOrElse(0.0).toInt)
    assertResult(-41, "Wrong Expense/Rev")((boRecord.expensePerTotalRevenue.getOrElse(0.0) * 100).toInt)

    val totalRecord = financeData.brokerages
      .filter(_.dimension.getOrElse().equals("ALL"))
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("01/2022"))
      .head
    assertResult(103, "Wrong number of transactions")(totalRecord.numOfTransactions.getOrElse(0))
    assertResult(0, "Wrong gross transaction value")(totalRecord.grossTransactionValue.getOrElse(0))
    assertResult(11605, "Wrong revenue")(totalRecord.revenue.getOrElse(0.0))
    assertResult(-8718, "Wrong COS - commission")(totalRecord.cog.getOrElse(0.0))
    assertResult(2887, "Wrong Gross profit")(totalRecord.grossProfit.getOrElse(0.0).toInt)
    assertResult(24, "Wrong Gross profit/Rev")((totalRecord.grossProfitPerRevenue.getOrElse(0.0) * 100).toInt)
    assertResult(-1654, "Wrong Selling & Marketing")(totalRecord.sellingAndMarketing.getOrElse(0.0).toInt)
    assertResult(-14, "Wrong Selling & Marketing/Rev")(
      (totalRecord.sellingAndMarketingPerRevenue.getOrElse(0.0) * 100).toInt
    )
    assertResult(-7560, "Wrong Personel, Office & Operation")(totalRecord.personnelOfficeOperation.getOrElse(0.0).toInt)
    assertResult(-65, "Wrong Personel, Office & Operation/Rev")(
      (totalRecord.personnelOfficeOperationPerRevenue.getOrElse(0.0) * 100).toInt
    )
    assertResult(-6327, "Wrong P&L")(totalRecord.profitAndLoss.getOrElse(0.0).toInt)
    assertResult(-54, "Wrong P&L/Rev")((totalRecord.profitAndLossPerRevenue.getOrElse(0.0) * 100).toInt)
    assertResult(-4407, "Wrong P&L Before Tech")(totalRecord.profitAndLossBeforeTech.getOrElse(0.0).toInt)
    assertResult(-37, "Wrong P&L Before Tech/Rev")(
      (totalRecord.profitAndLossBeforeTechPerRevenue.getOrElse(0.0) * 100).toInt
    )

  }

}
