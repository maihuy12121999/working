package rever.etl.data_sync.jobs.finance

import rever.etl.data_sync.client.GSheetClient
import rever.etl.data_sync.domain.finance._
import rever.etl.data_sync.util.FinanceGSheetUtils
import rever.etl.rsparkflow.utils.TimestampUtils

import scala.collection.JavaConverters.asScalaBufferConverter

trait DealReader extends BaseGSheetReader[DealRecord] {
  def read(spreadsheetId: String, sheet: String): Seq[DealRecord]
}

case class DealReaderImpl(client: GSheetClient, dataRange: String) extends DealReader {

  override def read(spreadsheetId: String, sheet: String): Seq[DealRecord] = {

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

    sheetDataTable.flatMap(row => {
      val monthInTime: Long = row.headOption.map(_.toString).map(TimestampUtils.parseMillsFromString(_, "MMM-yy")).get
      val dealClosedRow = row.slice(1, 7)
      val dealBookingRow = row.slice(9, 15)
      toDealBrokerageRecords("closed", monthInTime, dealClosedRow) ++
        toDealBrokerageRecords("booking", monthInTime, dealBookingRow)
    })

  }

  private def toDealBrokerageRecords(dealState: String, monthInTime: Long, sheetRowValues: Seq[AnyRef]): Seq[DealRecord] = {
    //Primary
    val primarySaleTransactions: Int =
      Math.round((sheetRowValues lift 0).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)).toInt
    val primaryTransactions = primarySaleTransactions

    val primarySaleRevenue = (sheetRowValues lift 3).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val primaryRevenue = primarySaleRevenue

    //Secondary
    val secondarySaleTransactions: Int =
      Math.round((sheetRowValues lift 1).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)).toInt
    val secondaryLeaseTransactions =
      Math.round((sheetRowValues lift 2).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)).toInt
    val secondaryTransactions = secondarySaleTransactions + secondaryLeaseTransactions

    val secondarySaleRevenue = (sheetRowValues lift 4).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val secondaryLeaseRevenue = (sheetRowValues lift 5).map(FinanceGSheetUtils.asDouble).getOrElse(0.0)
    val secondaryRevenue = secondarySaleRevenue + secondaryLeaseRevenue

    Seq(
      DealRecord(
        businessUnit = Some("PRIMARY"),
        dealState = Some(dealState),
        periodType = Some("MONTH"),
        periodValue = Some(TimestampUtils.asMonth(monthInTime)),
        totalTransactions = Some(primaryTransactions),
        saleTransactions = Some(primarySaleTransactions),
        leaseTransactions = Some(0),
        totalRevenue = Some(primaryRevenue),
        saleRevenue = Some(primarySaleRevenue),
        leaseRevenue = Some(0),
        time = Some(monthInTime),
        logTime = Some(System.currentTimeMillis())
      ),
      DealRecord(
        businessUnit = Some("SECONDARY"),
        dealState = Some(dealState),
        periodType = Some("MONTH"),
        periodValue = Some(TimestampUtils.asMonth(monthInTime)),
        totalTransactions = Some(secondaryTransactions),
        saleTransactions = Some(secondarySaleTransactions),
        leaseTransactions = Some(secondaryLeaseTransactions),
        totalRevenue = Some(secondaryRevenue),
        saleRevenue = Some(secondarySaleRevenue),
        leaseRevenue = Some(secondaryLeaseRevenue),
        time = Some(monthInTime),
        logTime = Some(System.currentTimeMillis())
      )
    )

  }

}
