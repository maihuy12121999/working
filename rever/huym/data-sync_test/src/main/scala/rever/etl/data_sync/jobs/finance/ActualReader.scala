package rever.etl.data_sync.jobs.finance

import org.scalatest.Matchers.assertResult
import rever.etl.data_sync.client.GSheetClient
import rever.etl.data_sync.domain.finance._
import rever.etl.data_sync.util.FinanceGSheetUtils
import rever.etl.rsparkflow.utils.TimestampUtils

import scala.collection.JavaConverters.asScalaBufferConverter

object ActualOrBudgetReader {

  private final val BROKERAGE_NO_TRANS_IDX = 0
  private final val BROKERAGE_GROSS_TRANS_VALUE_IDX = 1
  private final val BROKERAGE_REVENUE_IDX = 2
  private final val BROKERAGE_COS_IDX = 4
  private final val BROKERAGE_GROSS_PROFIT_IDX = 6
  private final val BROKERAGE_GROSS_PROFIT_PER_REVENUE_IDX = 7
  private final val BROKERAGE_SELLING_MARKETING_IDX = 8
  private final val BROKERAGE_SELLING_MARKETING_PER_REVENUE_IDX = 9
  private final val BROKERAGE_PERSONNEL_OFFICE_OPERATION_IDX = 10
  private final val BROKERAGE_PERSONNEL_OFFICE_OPERATION_PER_REVENUE_IDX = 11
  private final val BROKERAGE_PROFIT_AND_LOSS_IDX = 12
  private final val BROKERAGE_PROFIT_AND_LOSS_PER_REVENUE_IDX = 13

  private final val NON_BROKERAGE_REVENUE_IDX = 0
  private final val NON_BROKERAGE_EXPENSE_IDX = 1
  private final val NON_BROKERAGE_OPERATING_PROFIT_AND_LOSS_IDX = 2

  private final val PLATFORM_TOOL_SERVER_IDX = 0
  private final val PLATFORM_MARKETING_IDX = 1
  private final val PLATFORM_PERSONNEL_IDX = 2
  private final val PLATFORM_TECH_IDX = 3
  private final val PLATFORM_OTHER_DEPT_IDX = 4
  private final val PLATFORM_OFFICE_OPERATION_IDX = 5
  private final val PLATFORM_INTEREST_AND_OTHER_GAIN_LOSS_IDX = 6
  private final val PLATFORM_EXPENSE_IDX = 7
  private final val PLATFORM_EXPENSE_PER_TOTAL_REVENUE_IDX = 8

  private final val TOTAL_NO_TRANSACTION_IDX = 0
  private final val TOTAL_GROSS_TRANS_VALUE_IDX = 1
  private final val TOTAL_REVENUE_IDX = 2
  private final val TOTAL_COG_IDX = 3
  private final val TOTAL_GROSS_PROFIT_IDX = 4
  private final val TOTAL_GROSS_PROFIT_PER_REVENUE_IDX = 5
  private final val TOTAL_SELLING_MARKETING_IDX = 6
  private final val TOTAL_SELLING_MARKETING_PER_REVENUE_IDX = 7
  private final val TOTAL_PERSONNEL_OFFICE_OPERATION_IDX = 8
  private final val TOTAL_PERSONNEL_OFFICE_OPERATION_PER_REVENUE_IDX = 9
  private final val TOTAL_NET_PROFIT_AND_LOSS_IDX = 10
  private final val TOTAL_NET_PROFIT_AND_LOSS_PER_REVENUE_IDX = 11
  private final val TOTAL_NET_PROFIT_AND_LOSS_BEFORE_TECH_IDX = 12
  private final val TOTAL_NET_PROFIT_AND_LOSS_BEFORE_TECH_PER_REVENUE_IDX = 13

  def toTotalReverRecord(kind: String, monthInTime: Long, sheetRowValues: Seq[AnyRef]): BrokerageRecord = {
    val totalTransactions: Int =
      Math.round((sheetRowValues lift TOTAL_NO_TRANSACTION_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)).toInt

    val grossTransactionValue: Int =
      (sheetRowValues lift TOTAL_GROSS_TRANS_VALUE_IDX).map(FinanceGSheetUtils.asDouble).map(_.toInt).getOrElse(0)
    val revenue: Int =
      (sheetRowValues lift TOTAL_REVENUE_IDX).map(FinanceGSheetUtils.asDouble).map(_.toInt).getOrElse(0)
    val cog = (sheetRowValues lift TOTAL_COG_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val grossProfit = (sheetRowValues lift TOTAL_GROSS_PROFIT_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val grossProfitPerRevenue =
      (sheetRowValues lift TOTAL_GROSS_PROFIT_PER_REVENUE_IDX).map(FinanceGSheetUtils.asPercentInDouble).getOrElse(0.0)
    val sellingAndMarketing =
      (sheetRowValues lift TOTAL_SELLING_MARKETING_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val sellingAndMarketingPerRevenue =
      (sheetRowValues lift TOTAL_SELLING_MARKETING_PER_REVENUE_IDX)
        .map(FinanceGSheetUtils.asPercentInDouble)
        .getOrElse(0.0)
    val personnelOfficeOperation =
      (sheetRowValues lift TOTAL_PERSONNEL_OFFICE_OPERATION_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val personnelOfficeOperationPerRevenue =
      (sheetRowValues lift TOTAL_PERSONNEL_OFFICE_OPERATION_PER_REVENUE_IDX)
        .map(FinanceGSheetUtils.asPercentInDouble)
        .getOrElse(0.0)
    val netProfitAndLoss =
      (sheetRowValues lift TOTAL_NET_PROFIT_AND_LOSS_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val netProfitAndLossPerRevenue =
      (sheetRowValues lift TOTAL_NET_PROFIT_AND_LOSS_PER_REVENUE_IDX)
        .map(FinanceGSheetUtils.asPercentInDouble)
        .getOrElse(0.0)

    val netProfitAndLossBeforeTech =
      (sheetRowValues lift TOTAL_NET_PROFIT_AND_LOSS_BEFORE_TECH_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val netProfitAndLossBeforeTechPerRevenue =
      (sheetRowValues lift TOTAL_NET_PROFIT_AND_LOSS_BEFORE_TECH_PER_REVENUE_IDX)
        .map(FinanceGSheetUtils.asPercentInDouble)
        .getOrElse(0.0)

    BrokerageRecord(
      kind = Some(kind),
      dimension = Some("ALL"),
      periodType = Some("MONTH"),
      periodValue = Some(TimestampUtils.asMonth(monthInTime)),
      numOfTransactions = Some(totalTransactions),
      grossTransactionValue = Some(grossTransactionValue),
      revenue = Some(revenue),
      cog = Some(cog),
      grossProfit = Some(grossProfit),
      grossProfitPerRevenue = Some(grossProfitPerRevenue),
      sellingAndMarketing = Some(sellingAndMarketing),
      sellingAndMarketingPerRevenue = Some(sellingAndMarketingPerRevenue),
      personnelOfficeOperation = Some(personnelOfficeOperation),
      personnelOfficeOperationPerRevenue = Some(personnelOfficeOperationPerRevenue),
      profitAndLoss = Some(netProfitAndLoss),
      profitAndLossPerRevenue = Some(netProfitAndLossPerRevenue),
      profitAndLossBeforeTech = Some(netProfitAndLossBeforeTech),
      profitAndLossBeforeTechPerRevenue = Some(netProfitAndLossBeforeTechPerRevenue),
      time = Some(monthInTime),
      logTime = Some(System.currentTimeMillis())
    )

  }

  def toActualTotalBURecord(
      monthInTime: Long,
      sheetRowValues: Seq[AnyRef],
      totalReverReport: Option[BrokerageRecord]
  ): BURecord = {
    val numOfTransactions = totalReverReport.flatMap(_.numOfTransactions).getOrElse(0)
    val grossTransactionValue = totalReverReport.flatMap(_.grossTransactionValue).getOrElse(0.0)
    val revenue = totalReverReport.flatMap(_.revenue).getOrElse(0.0)
    val commission = Math.abs(totalReverReport.flatMap(_.cog).getOrElse(0.0))
    val grossProfit = revenue - commission
    val cos = (sheetRowValues lift 0).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val marketing = (sheetRowValues lift 1).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val cosPerRevenue = if (revenue != 0) cos / revenue else 0.0
    val marketingPerRevenue = if (revenue != 0) marketing / revenue else 0.0

    val personnel = (sheetRowValues lift 2).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val officeOperation = (sheetRowValues lift 3).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val personnelPerRevenue = if (revenue != 0) personnel / revenue else 0.0
    val officeOperationPerRevenue = if (revenue != 0) officeOperation / revenue else 0.0

    val profitAndLoss = totalReverReport.flatMap(_.profitAndLoss).getOrElse(0.0)
    val profitAndLossPerRevenue = if (revenue != 0) profitAndLoss / revenue else 0.0

    BURecord(
      kind = Some("actual"),
      dimension = Some("ALL"),
      periodType = Some("MONTH"),
      periodValue = Some(TimestampUtils.asMonth(monthInTime)),
      numOfTransactions = Some(numOfTransactions),
      grossTransactionValue = Some(grossTransactionValue),
      revenue = Some(revenue),
      commission = Some(commission),
      grossProfit = Some(grossProfit),
      selling = Some(cos),
      marketing = Some(marketing),
      cosPerRevenue = Some(cosPerRevenue),
      marketingPerRevenue = Some(marketingPerRevenue),
      personnel = Some(personnel),
      rental = Some(0.0),
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

  def toBrokerageRecord(
      kind: String,
      monthInTime: Long,
      dimension: String,
      sheetRowValues: Seq[AnyRef]
  ): BrokerageRecord = {
    val numOfTransactions: Int =
      Math.round((sheetRowValues lift BROKERAGE_NO_TRANS_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)).toInt
    val grossTransactionValue =
      (sheetRowValues lift BROKERAGE_GROSS_TRANS_VALUE_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val revenue = (sheetRowValues lift BROKERAGE_REVENUE_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val cos = (sheetRowValues lift BROKERAGE_COS_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val grossProfit = (sheetRowValues lift BROKERAGE_GROSS_PROFIT_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val grossProfitPerRevenue =
      (sheetRowValues lift BROKERAGE_GROSS_PROFIT_PER_REVENUE_IDX)
        .map(FinanceGSheetUtils.asPercentInDouble)
        .getOrElse(0.0)
    val sellingAndMarketing =
      (sheetRowValues lift BROKERAGE_SELLING_MARKETING_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val sellingAndMarketingPerRevenue =
      (sheetRowValues lift BROKERAGE_SELLING_MARKETING_PER_REVENUE_IDX)
        .map(FinanceGSheetUtils.asPercentInDouble)
        .getOrElse(0.0)
    val personnelOfficeOperation =
      (sheetRowValues lift BROKERAGE_PERSONNEL_OFFICE_OPERATION_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val personnelOfficeOperationPerRevenue =
      (sheetRowValues lift BROKERAGE_PERSONNEL_OFFICE_OPERATION_PER_REVENUE_IDX)
        .map(FinanceGSheetUtils.asPercentInDouble)
        .getOrElse(0.0)

    val profitAndLoss =
      (sheetRowValues lift BROKERAGE_PROFIT_AND_LOSS_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val profitAndLossPerRevenue =
      (sheetRowValues lift BROKERAGE_PROFIT_AND_LOSS_PER_REVENUE_IDX)
        .map(FinanceGSheetUtils.asPercentInDouble)
        .getOrElse(0.0)

    BrokerageRecord(
      kind = Some(kind),
      dimension = Some(dimension),
      periodType = Some("MONTH"),
      periodValue = Some(TimestampUtils.asMonth(monthInTime)),
      numOfTransactions = Some(numOfTransactions),
      grossTransactionValue = Some(grossTransactionValue),
      revenue = Some(revenue),
      cog = Some(cos),
      grossProfit = Some(grossProfit),
      grossProfitPerRevenue = Some(grossProfitPerRevenue),
      sellingAndMarketing = Some(sellingAndMarketing),
      sellingAndMarketingPerRevenue = Some(sellingAndMarketingPerRevenue),
      personnelOfficeOperation = Some(personnelOfficeOperation),
      personnelOfficeOperationPerRevenue = Some(personnelOfficeOperationPerRevenue),
      profitAndLoss = Some(profitAndLoss),
      profitAndLossPerRevenue = Some(profitAndLossPerRevenue),
      profitAndLossBeforeTech = Some(0.0),
      profitAndLossBeforeTechPerRevenue = Some(0.0),
      time = Some(monthInTime),
      logTime = Some(System.currentTimeMillis())
    )

  }

  def toNonBrokerageRecord(kind: String, monthInTime: Long, sheetRowValues: Seq[AnyRef]): NonBrokerageRecord = {
    val revenue = (sheetRowValues lift NON_BROKERAGE_REVENUE_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val expense = (sheetRowValues lift NON_BROKERAGE_EXPENSE_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val profitAndLoss =
      (sheetRowValues lift NON_BROKERAGE_OPERATING_PROFIT_AND_LOSS_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)

    NonBrokerageRecord(
      kind = Some(kind),
      periodType = Some("MONTH"),
      periodValue = Some(TimestampUtils.asMonth(monthInTime)),
      revenue = Some(revenue),
      expense = Some(expense),
      operatingProfitAndLoss = Some(profitAndLoss),
      time = Some(monthInTime),
      logTime = Some(System.currentTimeMillis())
    )

  }

  def toPlatformRecord(kind: String, monthInTime: Long, sheetRowValues: Seq[AnyRef]): PlatformRecord = {
    val toolAndServer: Int =
      (sheetRowValues lift PLATFORM_TOOL_SERVER_IDX).map(FinanceGSheetUtils.asDouble).map(_.toInt).getOrElse(0)
    val marketing = (sheetRowValues lift PLATFORM_MARKETING_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val personnel = (sheetRowValues lift PLATFORM_PERSONNEL_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val tech = (sheetRowValues lift PLATFORM_TECH_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val otherDept = (sheetRowValues lift PLATFORM_OTHER_DEPT_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val officeOperation =
      (sheetRowValues lift PLATFORM_OFFICE_OPERATION_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val interestAndOtherGainLoss =
      (sheetRowValues lift PLATFORM_INTEREST_AND_OTHER_GAIN_LOSS_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val expense =
      (sheetRowValues lift PLATFORM_EXPENSE_IDX).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val expensesPerTotalRevenue =
      (sheetRowValues lift PLATFORM_EXPENSE_PER_TOTAL_REVENUE_IDX)
        .map(FinanceGSheetUtils.asPercentInDouble)
        .getOrElse(0.0)

    PlatformRecord(
      kind = Some(kind),
      periodType = Some("MONTH"),
      periodValue = Some(TimestampUtils.asMonth(monthInTime)),
      toolAndServer = Some(toolAndServer),
      marketing = Some(marketing),
      personnel = Some(personnel),
      tech = Some(tech),
      otherDept = Some(otherDept),
      officeOperation = Some(officeOperation),
      interestAndOtherGainLoss = Some(interestAndOtherGainLoss),
      expense = Some(expense),
      expensePerTotalRevenue = Some(expensesPerTotalRevenue),
      time = Some(monthInTime),
      logTime = Some(System.currentTimeMillis())
    )

  }

}

trait ActualOrBudgetReader extends BaseGSheetReader[FinanceData] {
  val kind: String

  def read(spreadsheetId: String, sheet: String): Seq[FinanceData]

}

case class ActualReaderImpl(client: GSheetClient, dataRange: String) extends ActualOrBudgetReader {

  val kind = "actual"

  override def read(spreadsheetId: String, sheet: String): Seq[FinanceData] = {

    val sheetDataTable: Seq[Seq[AnyRef]] = client
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
      .toSeq

    val brokerages = readTotalRvReports(kind, sheetDataTable) ++ readPrimaryAndSecondary(kind, sheetDataTable)
    val nonBrokerages = readNonBrokerageReports(kind, sheetDataTable)
    val backOffices = readPlatformReport(kind, sheetDataTable)

    val data = FinanceData(brokerages, nonBrokerages, backOffices)
    testData(data)
    Seq(data)
  }

  private def readTotalRvReports(kind: String, sheetDataTable: Seq[Seq[AnyRef]]): Seq[BrokerageRecord] = {
    sheetDataTable.map(row => {
      val monthInTime: Long = row.headOption.map(_.toString).map(TimestampUtils.parseMillsFromString(_, "MMM-yy")).get
      val rvFinanceRow = row.slice(61, 75)

      ActualOrBudgetReader.toTotalReverRecord(kind, monthInTime, rvFinanceRow)
    })
  }

  private def readPrimaryAndSecondary(kind: String, sheetDataTable: Seq[Seq[AnyRef]]): Seq[BrokerageRecord] = {
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

  private def readNonBrokerageReports(kind: String, sheetDataTable: Seq[Seq[AnyRef]]): Seq[NonBrokerageRecord] = {
    sheetDataTable.map(row => {
      val monthInTime: Long = row.headOption.map(_.toString).map(TimestampUtils.parseMillsFromString(_, "MMM-yy")).get
      val nonBrokerageRow = row.slice(32, 35)

      val record = ActualOrBudgetReader.toNonBrokerageRecord(kind, monthInTime, nonBrokerageRow)

      record
    })
  }

  private def readPlatformReport(kind: String, sheetDataTable: Seq[Seq[AnyRef]]): Seq[PlatformRecord] = {
    sheetDataTable.map(row => {
      val monthInTime: Long = row.headOption.map(_.toString).map(TimestampUtils.parseMillsFromString(_, "MMM-yy")).get
      val platformRow = row.slice(51, 60)

      ActualOrBudgetReader.toPlatformRecord(kind, monthInTime, platformRow)
    })
  }

  /** This method is used to validate data is correct
    */
  private def testData(financeData: FinanceData): Unit = {

    val totalRecord = financeData.brokerages
      .filter(_.dimension.getOrElse().equals("ALL"))
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("01/2022"))
      .head

    assertResult(totalRecord.numOfTransactions.getOrElse(0), "Wrong number of transactions")(109)
    assertResult(totalRecord.grossTransactionValue.getOrElse(0), "Wrong gross transaction value")(872548)

    assertResult(totalRecord.revenue.getOrElse(0.0), "Wrong revenue")(11605.0)
    assertResult(totalRecord.cog.getOrElse(0.0), "Wrong COS - commission")(-8718.0)

    assertResult(totalRecord.grossProfit.getOrElse(0.0), "Wrong Gross profit")(2887.0)
    assertResult((totalRecord.grossProfitPerRevenue.getOrElse(0.0) * 100).toInt, "Wrong Gross profit/Rev")(24)

    assertResult(totalRecord.sellingAndMarketing.getOrElse(0.0), "Wrong Selling & Marketing")(-1653)
    assertResult(
      (totalRecord.sellingAndMarketingPerRevenue.getOrElse(0.0) * 100).toInt,
      "Wrong Selling & Marketing/Rev"
    )(-14)
    assertResult(totalRecord.personnelOfficeOperation.getOrElse(0.0), "Wrong Personel, Office & Operation")(-6107.88)

    assertResult(
      (totalRecord.personnelOfficeOperationPerRevenue.getOrElse(0.0) * 100).toInt,
      "Wrong Personel, Office & Operation/Rev"
    )(-52)

    assertResult(totalRecord.profitAndLoss.getOrElse(0.0), "Wrong P&L")(-4873.88)
    assertResult((totalRecord.profitAndLossPerRevenue.getOrElse(0.0) * 100).toInt, "Wrong P&L/Rev")(-41)

    assertResult(totalRecord.profitAndLossBeforeTech.getOrElse(0.0), "Wrong P&L Before Tech")(-3435.0)

    assertResult(
      (totalRecord.profitAndLossBeforeTechPerRevenue.getOrElse(0.0) * 100).toInt,
      "Wrong P&L Before Tech/Rev"
    )(-29)

    // Primary
    val pRecord = financeData.brokerages
      .filter(_.dimension.exists(_.equals("PRIMARY")))
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("01/2022"))
      .head

    assertResult(pRecord.numOfTransactions.getOrElse(0), "Wrong number of transactions")(58)
    assertResult(pRecord.grossTransactionValue.getOrElse(0.0), "Wrong gross transaction value")(652391)
    assertResult(pRecord.revenue.getOrElse(0.0), "Wrong revenue")(10733)
    assertResult(pRecord.cog.getOrElse(0.0), "Wrong COS - commission")(-8257.0)
    assertResult(pRecord.grossProfit.getOrElse(0.0), "Wrong Gross profit")( 2476.0)
    assertResult((pRecord.grossProfitPerRevenue.getOrElse(0.0) * 100).toInt, "Wrong Gross profit/Rev")(23)
    assertResult(pRecord.sellingAndMarketing.getOrElse(0.0), "Wrong Selling & Marketing")(-770)
    assertResult((pRecord.sellingAndMarketingPerRevenue.getOrElse(0.0) * 100).toInt, "Wrong Selling & Marketing/Rev")(
      -7
    )
    assertResult(pRecord.personnelOfficeOperation.getOrElse(0.0), "Wrong Personel, Office & Operation")(-1425)
    assertResult(
      (pRecord.personnelOfficeOperationPerRevenue.getOrElse(0.0) * 100).toInt,
      "Wrong Personel, Office & Operation/Rev"
    )(-13)

    assertResult(pRecord.profitAndLoss.getOrElse(0.0), "Wrong P&L")(281.0)
    assertResult((pRecord.profitAndLossPerRevenue.getOrElse(0.0) * 100).toInt, "Wrong P&L/Rev")(2)
    assertResult(pRecord.profitAndLossBeforeTech.getOrElse(0.0), "Wrong P&L Before Tech")(0.0)
    assertResult(
      (pRecord.profitAndLossBeforeTechPerRevenue.getOrElse(0.0) * 100).toInt,
      "Wrong P&L Before Tech/Rev"
    )(0)

    // Secondary
    val sRecord = financeData.brokerages
      .filter(_.dimension.exists(_.equals("SECONDARY")))
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("01/2022"))
      .head
    assertResult(sRecord.numOfTransactions.getOrElse(0), "Wrong number of transactions")(51)
    assertResult(sRecord.grossTransactionValue.getOrElse(0.0), "Wrong gross transaction value")(220157.2)
    assertResult(sRecord.revenue.getOrElse(0.0), "Wrong revenue")(872)
    assertResult(sRecord.cog.getOrElse(0.0), "Wrong COS - commission")(-461)
    assertResult(sRecord.grossProfit.getOrElse(0.0), "Wrong Gross profit")(411)
    assertResult((sRecord.grossProfitPerRevenue.getOrElse(0.0) * 100).toInt, "Wrong Gross profit/Rev")(47)
    assertResult(sRecord.sellingAndMarketing.getOrElse(0.0), "Wrong Selling & Marketing")(-275.0)
    assertResult((sRecord.sellingAndMarketingPerRevenue.getOrElse(0.0) * 100).toInt, "Wrong Selling & Marketing/Rev")(
      -31
    )
    assertResult(sRecord.personnelOfficeOperation.getOrElse(0.0), "Wrong Personel, Office & Operation")(-1186)
    assertResult(
      (sRecord.personnelOfficeOperationPerRevenue.getOrElse(0.0) * 100).toInt,
      "Wrong Personel, Office & Operation/Rev"
    )(-136)
    assertResult(sRecord.profitAndLoss.getOrElse(0.0), "Wrong P&L")(-1050.0)
    assertResult((sRecord.profitAndLossPerRevenue.getOrElse(0.0) * 100).toInt, "Wrong P&L/Rev")(-120)
    assertResult(sRecord.profitAndLossBeforeTech.getOrElse(0.0), "Wrong P&L Before Tech")(0.0)
    assertResult(
      (sRecord.profitAndLossBeforeTechPerRevenue.getOrElse(0.0) * 100).toInt,
      "Wrong P&L Before Tech/Rev"
    )(0)

    //Nonbrokage
    val nonRecord = financeData.nonBrokerages
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("05/2021"))
      .head
    assertResult(nonRecord.revenue.getOrElse(0), "Wrong revenue")(390)
    assertResult(nonRecord.expense.getOrElse(0.0), "Wrong expense")(-215)
    assertResult(nonRecord.operatingProfitAndLoss.getOrElse(0.0), "Wrong P&L")(175)

    val boRecord = financeData.backOffices
      .filter(_.periodType.getOrElse().equals("MONTH"))
      .filter(_.periodValue.getOrElse().equals("01/2022"))
      .head
    assertResult(boRecord.toolAndServer.getOrElse(0), "Wrong tool & server")(-402)
    assertResult(boRecord.marketing.getOrElse(0.0), "Wrong mkt")(-608)
    assertResult(boRecord.personnel.getOrElse(0.0).toInt, "Wrong personel")(-3287)
    assertResult(boRecord.tech.getOrElse(0.0).toInt, "Wrong tech")(-1036)
    assertResult(boRecord.otherDept.getOrElse(0.0).toInt, "Wrong other dept")(-2251)
    assertResult(boRecord.officeOperation.getOrElse(0.0).toInt, "Wrong office operation")(-405)
    assertResult(boRecord.interestAndOtherGainLoss.getOrElse(0.0).toInt, "Wrong Interest and other gain losss")(598)
    assertResult(boRecord.expense.getOrElse(0.0).toInt, "Wrong expense")(-4104)
    assertResult(
      (boRecord.expensePerTotalRevenue.getOrElse(0.0) * 100).toInt,
      "Wrong Expense/Rev"
    )(0)

  }
}
