package rever.etl.data_sync.core.clickhouse

import rever.etl.data_sync.domain.finance._
import vn.rever.jdbc.JdbcRecord
import vn.rever.jdbc.clickhouse.ClickHouseJdbcDAO
import vn.rever.jdbc.mysql.MySqlDAO

import java.sql.Connection
import javax.sql.DataSource

abstract class CustomChDAO[T <: JdbcRecord] extends ClickHouseJdbcDAO[T] {

  //TODO: Hotfix for current Clickhouse JDBC driver always return 1 for DML statement.
  override def multiInsert(rows: Seq[T], connection: Option[Connection]): Int = {
    val result = super.multiInsert(rows, connection)
    if (rows.nonEmpty && result > 0) {
      rows.size
    } else {
      0
    }
  }

  def optimizeTable(): Unit = {
    execute {
      executeUpdate(s"OPTIMIZE TABLE ${table} FINAL")(_)
    }
  }


}
case class CHCostFinanceDAO(ds: DataSource) extends CustomChDAO[CostRecord] {

  override val table: String = CostRecord.TBL_NAME

  override def createRecord(): CostRecord = CostRecord.empty

  def batchInsert(items: Seq[CostRecord]): Int = {
    multiInsert(items)
  }

  def optimizeDeduplicateData(): Unit = {
    super.optimizeTable()
  }

}

case class CHDealBrokerageRecordDAO(ds: DataSource) extends CustomChDAO[DealRecord] {

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
