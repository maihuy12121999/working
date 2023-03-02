package rever.etl.data_sync.jobs.finance

import rever.etl.data_sync.client.GSheetClient
import rever.etl.data_sync.core.Runner
import rever.etl.data_sync.core.clickhouse.ClickhouseSink
import rever.etl.data_sync.domain.finance.{BURecord, FinanceData}
import rever.etl.data_sync.repository._
import rever.etl.data_sync.util.JsonUtils
import rever.etl.rsparkflow.api.configuration.Config

import java.util.Base64

/** @author anhlt (andy)
  * @since 06/07/2022
  */

case class DataRangeConfig(
    actualRange: Option[String] = Some("C2:BW76"),
    byLevelRange: Option[String] = Some("A81:BW146"),
    budgetRange: Option[String] = Some("C2:Z76"),
    buActualRange: Option[String] = Some("C2:BZ33"),
    costCompositionRange: Option[String] = Some("C43:BZ78"),
    buBudgetRange: Option[String] = Some("C1:Z48"),
    dealRange: Option[String] = Some("G2:BJ16"),
    costRange: Option[String] = Some("C3:CL20"),
    bookingRange: Option[String] = Some("A:L")
)

case class FinanceDataToCH(config: Config) extends Runner {

  private val client = GSheetClient.client(
    new String(
      Base64.getDecoder.decode(config.get("google_service_account_encoded")),
      "UTF-8"
    )
  )
  private val financeSheetId = config.get("finance_data_sheet_id")
  private val dataRangeConfig = Option(config.get("data_range_config", ""))
    .filterNot(_.isEmpty)
    .map(JsonUtils.fromJson[DataRangeConfig](_))
    .getOrElse(DataRangeConfig())

  private val actualReader: ActualOrBudgetReader = ActualReaderImpl(
    client,
    dataRangeConfig.actualRange.getOrElse("C2:BW76")
  )

  private val budgetReader: ActualOrBudgetReader = BudgetReaderImpl(
    client,
    dataRangeConfig.budgetRange.getOrElse("C2:Z76")
  )
  private val buActualReader: BuActualReader = BuActualReaderImpl(
    financeSheetId,
    client,
    dataRangeConfig.buActualRange.getOrElse("C2:BX33"),
    actualReader,
    fieldMapping = Map(
      BURecord.NUM_OF_TRANSACTIONS -> 0,
      BURecord.GROSS_TRANSACTION_VALUE -> 1,
      BURecord.REVENUE -> 2,
      BURecord.COMMISSION -> 3,
      BURecord.SELLING -> 4,
      BURecord.MARKETING -> 5,
      BURecord.PERSONNEL -> 6,
      BURecord.OFFICE_OPERATION -> 7,
      BURecord.PROFIT_AND_LOSS -> 8
    )
  )

  private val buBudgetReader: BuBudgetReader = BuBudgetReaderImpl(
    client,
    dataRangeConfig.buBudgetRange.getOrElse("C1:Z48"),
    fieldMapping = Map(
      BURecord.NUM_OF_TRANSACTIONS -> 0,
      BURecord.GROSS_TRANSACTION_VALUE -> 1,
      BURecord.REVENUE -> 2,
      BURecord.COMMISSION -> 3,
      BURecord.SELLING -> 4,
      BURecord.MARKETING -> 5,
      BURecord.PERSONNEL -> 6,
      BURecord.RENTAL -> 7,
      BURecord.OFFICE_OPERATION -> 8,
      BURecord.PROFIT_AND_LOSS -> 9
    )
  )
  private val byLevelReader: ByLevelReader =
    ByLevelReaderImpl(client, dataRangeConfig.byLevelRange.getOrElse("A81:BW146"))
  private val dealReader: DealReader = DealReaderImpl(client, dataRangeConfig.dealRange.getOrElse("G2:BH16"))
  private val costReader: CostReader = CostReaderImpl(client, dataRangeConfig.costRange.getOrElse("C3:CJ20"))
  private val costCompositionReader: CostCompositionReader = CostCompositionReaderImpl(
    financeSheetId,
    client,
    dataRangeConfig.costCompositionRange.getOrElse("C43:BW78"),
    actualReader
  )
  private val bookingReader: BookingReader = BookingReaderImpl(client, dataRangeConfig.bookingRange.getOrElse("A:L"))
  private val teamSizeReader: BoTeamSizeReader = BoTeamSizeReaderImpl(
    client,
    dataRangeConfig.bookingRange.getOrElse("A:C"),
    Map(
      "time" -> 0,
      "team" -> 1,
      "team_size" -> 2
    )
  )

  private val boCostReader: BoCostReader = BoCostReaderImpl(
    client,
    dataRangeConfig.bookingRange.getOrElse("A:D"),
    Map(
      "Salary & Bonus" -> "salary_and_bonus",
      "Other Benefits" -> "other_benefit"
    ),
    Map(
      "Recruitment fee" -> "recruitment_fee",
      "Training cost" -> "training_cost",
      "Automobile Expense" -> "automobile_expense",
      "Lễ Tết / sự kiện + Team building + Chăm sóc sk + Đồng phục" -> "others",
      "Tech" -> "tech",
      "Other department" -> "other_department"
    )
  )

  private val incomeByLevelReader: IncomeByLevelReader = IncomeByLevelReaderImpl(
    client,
    dataRangeConfig.bookingRange.getOrElse("A:E"),
    Map(
      "primary" -> "PRIMARY",
      "secondary" -> "SECONDARY"
    ),
    Map(
      "sd" -> "sd",
      "sm" -> "sm",
      "sales admin" -> "sales_admin",
      "sales" -> "sales",
      "listing" -> "listing"
    )
  )

  private val ds = ClickhouseSink.client(config)
  private val brokerageFinanceDAO = CHBrokerageFinanceDAO(ds)
  private val nonBrokerageFinanceDAO = CHNonBrokerageFinanceDAO(ds)
  private val platformFinanceDAO = CHPlatformFinanceDAO(ds)
  private val buBrokerageDAO = CHBUBrokerageFinanceDAO(ds)
  private val dealDAO = CHDealDAO(ds)
  private val costDAO = CHCostDAO(ds)
  private val costCompositionDAO = CHCostCompositionDAO(ds)
  private val bookingDAO = CHBookingFinanceDAO(ds)
  private val byLevelDAO = CHByLevelFinanceDAO(ds)
  private val boTeamSizeDAO = CHBoTeamSizeDAO(ds)
  private val boCostDAO = CHBoCostDAO(ds)
  private val incomeByLevelDAO = CHIncomeByLevelDAO(ds)

  override def run(): Long = {

    val budgetCount = importBudgetFinanceSheet()
    val actualCount = importActualFinanceSheet()

    val buCount = importBUSheet()
    val dealCount = importDealSheet()
    val costCount = importCostSheet()
    val boCostCount = importBoCostSheet()
    val bookingCount = importBookingSheet()
    val byLevelCount = importByLevel()
    val teamSizeCount = importBoTeamSize()
    val incomeByLevelCount = importIncomeByJobLevel()
    val totalCount = Seq[Long](
      actualCount,
      byLevelCount,
      budgetCount,
      buCount,
      dealCount,
      costCount,
      boCostCount,
      bookingCount,
      teamSizeCount,
      incomeByLevelCount
    ).sum

    if (totalCount > 0) {
      brokerageFinanceDAO.optimizeDeduplicateData()
      nonBrokerageFinanceDAO.optimizeDeduplicateData()
      platformFinanceDAO.optimizeDeduplicateData()
      buBrokerageDAO.optimizeDeduplicateData()
      dealDAO.optimizeDeduplicateData()
      costDAO.optimizeDeduplicateData()
      bookingDAO.optimizeDeduplicateData()
      byLevelDAO.optimizeDeduplicateData()
      boTeamSizeDAO.optimizeDeduplicateData()
      boCostDAO.optimizeDeduplicateData()
      incomeByLevelDAO.optimizeDeduplicateData()
    }

    println(s"Inserted: ${totalCount} records from all sheets.")
    totalCount
  }

  private def importBookingSheet(): Long = {
    val bookingRecords = bookingReader.read(financeSheetId, "Booking")

    val totalInserted = bookingDAO.multiInsert(bookingRecords)

    println(s"Inserted: ${totalInserted} records from Booking sheet.")
    totalInserted
  }
  private def importBoTeamSize(): Long = {
    val records = teamSizeReader.read(financeSheetId, "Back Office Head Count")

    val totalInserted = boTeamSizeDAO.multiInsert(records)

    println(s"Inserted: ${totalInserted} records from BO TeamSize sheet.")
    totalInserted
  }

  private def importIncomeByJobLevel(): Long = {
    val records = incomeByLevelReader.read(financeSheetId, "Income by Level")

    val totalInserted = incomeByLevelDAO.multiInsert(records)

    println(s"Inserted: ${totalInserted} records from Income By Job Level sheet.")
    totalInserted
  }

  private def importActualFinanceSheet(): Long = {
    val financeData = actualReader.read(financeSheetId, "Actual")

    val totalInserted = insertFinanceData(actualReader.kind, financeData.head)

    println(s"Inserted: ${totalInserted} records from Actual sheet.")

    totalInserted
  }

  private def importBudgetFinanceSheet(): Long = {
    val financeData = budgetReader.read(financeSheetId, "Budget")
    val totalInserted = insertFinanceData(budgetReader.kind, financeData.head)
    println(s"Inserted: ${totalInserted} records from Budget sheet.")
    totalInserted
  }

  private def importByLevel(): Long = {
    val records = byLevelReader.read(financeSheetId, "Actual")

    val totalInserted = byLevelDAO.batchInsert(records)
    println(s"Inserted: ${totalInserted} by level records from Actual sheet.")

    totalInserted
  }

  private def importBUSheet(): Long = {

    val actualBuRecords = buActualReader.read(financeSheetId, "BU P&L Actual")

    val budgetBuRecords = buBudgetReader.read(financeSheetId, "BU P&L Budget")

    val actualCostCompositionRecords = costCompositionReader.read(financeSheetId, "BU P&L Actual")

    val totalBudgetCount = buBrokerageDAO.batchInsert(budgetBuRecords)
    val totalActualCount = buBrokerageDAO.batchInsert(actualBuRecords)
    val totalActualCostCompositionCount = costCompositionDAO.batchInsert(actualCostCompositionRecords)

    if (totalBudgetCount <= 0 && budgetBuRecords.nonEmpty) {
      throw new Exception(s"Can't import budget BU data")
    }

    if (totalActualCount <= 0 && actualBuRecords.nonEmpty) {
      throw new Exception(s"Can't import actual BU data")
    }

    val total = totalActualCount + totalBudgetCount + totalActualCostCompositionCount

    println(s"Inserted: ${total} records from BU Actual&Budget P/L sheet.")

    total

  }

  private def importDealSheet(): Long = {
    val dealBrokerageRecords = dealReader.read(financeSheetId, sheet = "Deals")

    val totalCount = dealDAO.batchInsert(dealBrokerageRecords)

    if (totalCount <= 0 && dealBrokerageRecords.nonEmpty) {
      throw new Exception(s"Can't import deal data")
    }

    println(s"Inserted: ${totalCount} records from Deal sheet.")

    totalCount
  }

  private def importCostSheet(): Long = {
    val buRecords = costReader.read(financeSheetId, "Cost")

    val totalCount = costDAO.batchInsert(buRecords)

    if (totalCount <= 0 && buRecords.nonEmpty) {
      throw new Exception(s"Can't import cost data")
    }

    println(s"Inserted: ${totalCount} records from Cost sheet.")

    totalCount
  }

  private def importBoCostSheet(): Long = {
    val records = boCostReader.read(financeSheetId, "Back Office Cost")

    val totalCount = boCostDAO.batchInsert(records)

    if (totalCount <= 0 && records.nonEmpty) {
      throw new Exception(s"Can't import bo cost data")
    }

    println(s"Inserted: ${totalCount} records from Bo Cost sheet.")

    totalCount
  }

  private def insertFinanceData(kind: String, financeData: FinanceData): Long = {

    val brokerageCount = brokerageFinanceDAO.batchInsert(financeData.brokerages)
    val nonBrokerageCount = nonBrokerageFinanceDAO.batchInsert(financeData.nonBrokerages)
    val platformCount = platformFinanceDAO.batchInsert(financeData.backOffices)

    if (brokerageCount <= 0 && financeData.brokerages.nonEmpty) {
      throw new Exception(s"Can't import brokerage finance data: $kind")
    }

    if (nonBrokerageCount <= 0 && financeData.nonBrokerages.nonEmpty) {
      throw new Exception(s"Can't import non brokerage finance data: $kind")
    }

    if (platformCount <= 0 && financeData.backOffices.nonEmpty) {
      throw new Exception(s"Can't import platform finance data: $kind")
    }

    brokerageCount + nonBrokerageCount + platformCount
  }

}
