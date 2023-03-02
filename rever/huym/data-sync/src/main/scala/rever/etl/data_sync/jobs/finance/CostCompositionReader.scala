package rever.etl.data_sync.jobs.finance

import rever.etl.data_sync.client.GSheetClient
import rever.etl.data_sync.domain.finance.AgentLevelReport
import rever.etl.data_sync.util.FinanceGSheetUtils
import rever.etl.rsparkflow.utils.TimestampUtils

import rever.etl.data_sync.client.GSheetClient
import rever.etl.data_sync.domain.finance._
import rever.etl.data_sync.util.FinanceGSheetUtils
import rever.etl.rsparkflow.utils.TimestampUtils

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ListBuffer

trait CostCompositionReader extends BaseGSheetReader[CostCompositionRecord] {
  def read(spreadsheetId: String, sheet: String): Seq[CostCompositionRecord]
}

case class CostCompositionReaderImpl(
    financeSheetId: String,
    client: GSheetClient,
    dataRange: String,
    actualReader: ActualOrBudgetReader
) extends CostCompositionReader {

  override def read(spreadsheetId: String, sheet: String): Seq[CostCompositionRecord] = {
    val (totalRevenueMap, primaryRevenueMap, secondaryRevenueMap) = readTotalReverAndRevenue()

    val sheetDataTable = client
      .getSheetValue(
        spreadsheetId,
        range = s"${sheet}!${dataRange}",
        isRow = false,
        renderOption = "UNFORMATTED_VALUE"
      )

    sheetDataTable.flatMap(row => {
      val monthInTime: Long = row.headOption.map(_.toString).map(TimestampUtils.parseMillsFromString(_, "MMM-yy")).get
      val totalRevenue = totalRevenueMap.getOrElse(s"MONTH_${TimestampUtils.asMonth(monthInTime)}", 0.0)
      val primaryRevenue = primaryRevenueMap.getOrElse(s"MONTH_${TimestampUtils.asMonth(monthInTime)}", 0.0)
      val secondaryRevenue = secondaryRevenueMap.getOrElse(s"MONTH_${TimestampUtils.asMonth(monthInTime)}", 0.0)

      val totalRow = row.slice(1, 12)
      val primaryRow = row.slice(14, 23)
      val secondaryRow = row.slice(26, 35)

      Seq(
        parseCompositionCost("ALL", monthInTime, totalRow, totalRevenue),
        parseCompositionCost("PRIMARY", monthInTime, primaryRow, primaryRevenue),
        parseCompositionCost("SECONDARY", monthInTime, secondaryRow, secondaryRevenue)
      )

    })

  }

  private def readTotalReverAndRevenue() = {

    val totalReverMap = actualReader
      .read(financeSheetId, "Actual")
      .head
      .brokerages
      .filter(_.dimension.exists(_.equals("ALL")))
      .map(r => {
        s"${r.periodType.getOrElse("")}_${r.periodValue.getOrElse("")}" -> r
      })
      .toMap
    val totalRevenueMap = totalReverMap.map(r => r._1 -> r._2.revenue.getOrElse(0.0))
    val primaryRevenueMap = actualReader
      .read(financeSheetId, "Actual")
      .head
      .brokerages
      .filter(_.dimension.exists(_.equalsIgnoreCase("PRIMARY")))
      .map(r => {
        s"${r.periodType.getOrElse("")}_${r.periodValue.getOrElse("")}" -> r.revenue.getOrElse(0.0)
      })
      .toMap

    val secondaryRevenueMap = actualReader
      .read(financeSheetId, "Actual")
      .head
      .brokerages
      .filter(_.dimension.exists(_.equalsIgnoreCase("SECONDARY")))
      .map(r => {
        s"${r.periodType.getOrElse("")}_${r.periodValue.getOrElse("")}" -> r.revenue.getOrElse(0.0)
      })
      .toMap

    (totalRevenueMap, primaryRevenueMap, secondaryRevenueMap)
  }

  private def parseCompositionCost(
      businessUnit: String,
      monthTime: Long,
      rowValues: Seq[AnyRef],
      totalRevenue: Double
  ): CostCompositionRecord = {

    val officeCost = (rowValues lift 0).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val depreciationCost = (rowValues lift 1).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val toolAndServerCost = (rowValues lift 2).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val serviceFee = (rowValues lift 3).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val totalCost = (rowValues lift 4).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)

    val officeCostPercent = (rowValues lift 6).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val depreciationCostPercent = (rowValues lift 7).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val toolAndServerCostPercent = (rowValues lift 8).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val serviceFeePercent = (rowValues lift 9).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val totalCostPercent = (rowValues lift 10).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)

    CostCompositionRecord(
      kind = Some("actual"),
      periodType = Some("MONTH"),
      periodValue = Some(TimestampUtils.asMonth(monthTime)),
      businessUnit = Some(businessUnit),
      revenue = Some(totalRevenue),
      office = Some(officeCost),
      depreciation = Some(depreciationCost),
      toolAndServer = Some(toolAndServerCost),
      serviceFee = Some(serviceFee),
      totalCost = Some(totalCost),
      time = Some(monthTime),
      logTime = Some(System.currentTimeMillis())
    )
  }
}
