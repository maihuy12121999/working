package rever.etl.data_sync.jobs.finance

import rever.etl.data_sync.client.GSheetClient
import rever.etl.data_sync.domain.finance.AgentLevelReport
import rever.etl.data_sync.util.FinanceGSheetUtils
import rever.etl.rsparkflow.utils.TimestampUtils

import scala.collection.JavaConverters.asScalaBufferConverter

trait ByLevelReader extends BaseGSheetReader[AgentLevelReport] {
  def read(spreadsheetId: String, sheet: String): Seq[AgentLevelReport]
}

case class ByLevelReaderImpl(client: GSheetClient, dataRange: String) extends ByLevelReader {

  override def read(spreadsheetId: String, sheet: String): Seq[AgentLevelReport] = {

    val sheetDataTable = client
      .getSheetValue(spreadsheetId, range = s"${sheet}!${dataRange}", isRow = false, renderOption = "UNFORMATTED_VALUE")
      .getValues
      .asScala
      .map(_.asScala.toSeq)
      .filterNot(_.isEmpty)

    val primaryServiceTypes = sheetDataTable(0).slice(1, 29).map(_.toString)
    val primaryLevelCategories = sheetDataTable(1).slice(1, 29).map(_.toString)

    val secondaryServiceTypes = sheetDataTable(0).slice(32, 66).map(_.toString)
    val secondaryLevelCategories = sheetDataTable(1).slice(32, 66).map(_.toString)

    sheetDataTable
      .drop(2)
      .flatMap(row => {
        val monthInTime: Long = row.headOption.map(_.toString).map(TimestampUtils.parseMillsFromString(_, "MMM-yy")).get

        val primaryServiceType = primaryServiceTypes.slice(0, 8)
        val primaryLevelCategory = primaryLevelCategories.slice(0, 8)
        val primaryRow = row.slice(1, 29)

        val secondaryServiceType = secondaryServiceTypes.slice(0, 10)
        val secondaryLevelCategory = secondaryLevelCategories.slice(0, 10)
        val secondaryRow = row.slice(32, 66)

        readPrimaryRecords(monthInTime, primaryRow, primaryServiceType, primaryLevelCategory) ++
          readSecondaryRecords(monthInTime, secondaryRow, secondaryServiceType, secondaryLevelCategory)

      })

  }

  private def readPrimaryRecords(
      monthInTime: Long,
      primaryRow: Seq[AnyRef],
      primaryServiceType: Seq[String],
      primaryLevelCategory: Seq[String]
  ): Seq[AgentLevelReport] = {
    val primaryRevenues = primaryRow.slice(0, 8)
    val primaryTransactions = primaryRow.slice(10, 18)
    val primaryCommissions = primaryRow.slice(20, 28)

    val primaryMetrics = primaryTransactions.zip(primaryRevenues).zip(primaryCommissions).map {
      case ((transaction, revenue), commission) => Seq(transaction, revenue, commission)
    }

    primaryMetrics
      .zip(primaryServiceType)
      .zip(primaryLevelCategory)
      .map { case ((metrics, serviceTypeName), levelName) =>
        val serviceType = FinanceGSheetUtils.getServiceType(serviceTypeName.toString)
        val level = FinanceGSheetUtils.getLevel(levelName.toString)

        AgentLevelReport(
          kind = Some("actual"),
          periodType = Some("MONTH"),
          periodValue = Some(TimestampUtils.asMonth(monthInTime)),
          businessUnit = Some("PRIMARY"),
          serviceType = Some(serviceType),
          categoryLevel = Some(level),
          revenue = Some(FinanceGSheetUtils.asDouble(metrics(1))),
          transaction = Some(FinanceGSheetUtils.asDouble(metrics(0)).toInt),
          commission = Some(FinanceGSheetUtils.asDouble(metrics(2))),
          time = Some(monthInTime),
          logTime = Some(System.currentTimeMillis())
        )
      }

  }

  private def readSecondaryRecords(
      monthInTime: Long,
      secondaryRow: Seq[AnyRef],
      secondaryServiceType: Seq[String],
      secondaryLevelCategory: Seq[String]
  ): Seq[AgentLevelReport] = {

    val secondaryRevenues = secondaryRow.slice(0, 10)
    val secondaryTransactions = secondaryRow.slice(12, 22)
    val secondaryCommissions = secondaryRow.slice(24, 34)
    val secondaryMetrics = secondaryTransactions.zip(secondaryRevenues).zip(secondaryCommissions).map {
      case ((transaction, revenue), commission) => Seq(transaction, revenue, commission)
    }

    secondaryMetrics
      .zip(secondaryServiceType)
      .zip(secondaryLevelCategory)
      .map { case ((metrics, serviceTypeName), levelName) =>
        val serviceType = FinanceGSheetUtils.getServiceType(serviceTypeName.toString)
        val level = FinanceGSheetUtils.getLevel(levelName.toString)

        AgentLevelReport(
          kind = Some("actual"),
          periodType = Some("MONTH"),
          periodValue = Some(TimestampUtils.asMonth(monthInTime)),
          businessUnit = Some("SECONDARY"),
          serviceType = Some(serviceType),
          categoryLevel = Some(level),
          revenue = Some(FinanceGSheetUtils.asDouble(metrics(1))),
          transaction = Some(FinanceGSheetUtils.asDouble(metrics(0)).toInt),
          commission = Some(FinanceGSheetUtils.asDouble(metrics(2))),
          time = Some(monthInTime),
          logTime = Some(System.currentTimeMillis())
        )
      }

  }

}
