package test_sync

import rever.etl.data_sync.domain.finance._
import vn.rever.jdbc.JdbcRecord
import vn.rever.jdbc.clickhouse.ClickHouseJdbcDAO
import vn.rever.jdbc.mysql.MySqlDAO

import java.sql.Connection
import javax.sql.DataSource

abstract class CustomChDAO[T <: JdbcRecord] extends MySqlDAO[T] {

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
case class MySqlStudentDAO(ds:DataSource) extends MySqlDAO[StudentRecord]{
  override def createRecord(): StudentRecord = StudentRecord()

  override def table: String = StudentRecord.TBL_NAME
}

case class CHStudentDAO(ds: DataSource) extends CustomChDAO[StudentRecord] {

  override val table: String = StudentRecord.TBL_NAME

  override def createRecord(): StudentRecord = StudentRecord()

  def batchInsert(items: Seq[StudentRecord]): Int = {
    multiInsert(items)
  }

  def optimizeDeduplicateData(): Unit = {
    super.optimizeTable()
  }

}

