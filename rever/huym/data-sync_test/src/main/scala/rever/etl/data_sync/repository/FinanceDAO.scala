package rever.etl.data_sync.repository

import rever.etl.data_sync.core.clickhouse.CustomChDAO
import rever.etl.data_sync.domain.finance._

import javax.sql.DataSource

case class CHCostDAO(ds: DataSource) extends CustomChDAO[CostRecord] {

  override val table: String = CostRecord.TBL_NAME

  override def createRecord(): CostRecord = CostRecord.empty

  def batchInsert(items: Seq[CostRecord]): Int = {
    multiInsert(items)
  }

  def optimizeDeduplicateData(): Unit = {
    super.optimizeTable()
  }

}

case class CHCostCompositionDAO(ds: DataSource) extends CustomChDAO[CostCompositionRecord] {

  override val table: String = CostCompositionRecord.TBL_NAME

  override def createRecord(): CostCompositionRecord = CostCompositionRecord.empty

  def batchInsert(items: Seq[CostCompositionRecord]): Int = {
    multiInsert(items)
  }

  def optimizeDeduplicateData(): Unit = {
    super.optimizeTable()
  }

}

case class CHDealDAO(ds: DataSource) extends CustomChDAO[DealRecord] {

  override val table: String = DealRecord.TBL_NAME

  override def createRecord(): DealRecord = DealRecord.empty

  def batchInsert(items: Seq[DealRecord]): Int = {
    multiInsert(items)
  }

  def optimizeDeduplicateData(): Unit = {
    super.optimizeTable()
  }

}

case class CHBrokerageFinanceDAO(ds: DataSource) extends CustomChDAO[BrokerageRecord] {

  override val table: String = BrokerageRecord.TBL_NAME

  override def createRecord(): BrokerageRecord = BrokerageRecord.empty

  def batchInsert(items: Seq[BrokerageRecord]): Int = {
    multiInsert(items)
  }

  def optimizeDeduplicateData(): Unit = {
    super.optimizeTable()
  }

}

case class CHBUBrokerageFinanceDAO(ds: DataSource) extends CustomChDAO[BURecord] {

  override val table: String = BURecord.TBL_NAME

  override def createRecord(): BURecord = BURecord.empty

  def batchInsert(items: Seq[BURecord]): Int = {
    multiInsert(items)
  }

  def optimizeDeduplicateData(): Unit = {
    super.optimizeTable()
  }

}

case class CHNonBrokerageFinanceDAO(ds: DataSource) extends CustomChDAO[NonBrokerageRecord] {

  override val table: String = NonBrokerageRecord.TBL_NAME

  override def createRecord(): NonBrokerageRecord = NonBrokerageRecord.empty

  def batchInsert(items: Seq[NonBrokerageRecord]): Int = {
    multiInsert(items)
  }

  def optimizeDeduplicateData(): Unit = {
    super.optimizeTable()
  }

}

case class CHPlatformFinanceDAO(ds: DataSource) extends CustomChDAO[PlatformRecord] {

  override val table: String = PlatformRecord.TBL_NAME

  override def createRecord(): PlatformRecord = PlatformRecord.empty

  def batchInsert(items: Seq[PlatformRecord]): Int = {
    multiInsert(items)
  }

  def optimizeDeduplicateData(): Unit = {
    super.optimizeTable()
  }

}

case class CHBookingFinanceDAO(ds: DataSource) extends CustomChDAO[BookingRecord] {

  override val table: String = BookingRecord.TBL_NAME

  override def createRecord(): BookingRecord = BookingRecord.empty

  def batchInsert(items: Seq[BookingRecord]): Int = {
    multiInsert(items)
  }

  def optimizeDeduplicateData(): Unit = {
    super.optimizeTable()
  }

}

case class CHByLevelFinanceDAO(ds: DataSource) extends CustomChDAO[AgentLevelReport] {

  override val table: String = AgentLevelReport.TBL_NAME

  override def createRecord(): AgentLevelReport = AgentLevelReport.empty

  def batchInsert(items: Seq[AgentLevelReport]): Int = {
    multiInsert(items)
  }

  def optimizeDeduplicateData(): Unit = {
    super.optimizeTable()
  }

}

case class CHBoTeamSizeDAO(ds: DataSource) extends CustomChDAO[BoTeamSize] {

  override val table: String = BoTeamSize.TBL_NAME

  override def createRecord(): BoTeamSize = BoTeamSize.empty

  def batchInsert(items: Seq[BoTeamSize]): Int = {
    multiInsert(items)
  }

  def optimizeDeduplicateData(): Unit = {
    super.optimizeTable()
  }

}

case class CHBoCostDAO(ds: DataSource) extends CustomChDAO[BoCostRecord] {

  override val table: String = BoCostRecord.TBL_NAME

  override def createRecord(): BoCostRecord = BoCostRecord.empty

  def batchInsert(items: Seq[BoCostRecord]): Int = {
    multiInsert(items)
  }

  def optimizeDeduplicateData(): Unit = {
    super.optimizeTable()
  }

}


case class CHIncomeByLevelDAO(ds: DataSource) extends CustomChDAO[IncomeByLevel] {

  override val table: String = IncomeByLevel.TBL_NAME

  override def createRecord(): IncomeByLevel = IncomeByLevel.empty

  def batchInsert(items: Seq[IncomeByLevel]): Int = {
    multiInsert(items)
  }

  def optimizeDeduplicateData(): Unit = {
    super.optimizeTable()
  }

}
