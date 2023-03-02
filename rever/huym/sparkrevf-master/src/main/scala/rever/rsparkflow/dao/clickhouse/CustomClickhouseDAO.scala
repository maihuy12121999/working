package rever.rsparkflow.dao.clickhouse

import vn.rever.jdbc.JdbcRecord
import vn.rever.jdbc.clickhouse.ClickHouseJdbcDAO

import java.sql.Connection

abstract class CustomClickhouseDAO[T <: JdbcRecord] extends ClickHouseJdbcDAO[T] {

  //TODO: Hotfix for current Clickhouse JDBC driver always return 1 for DML statement.
  override def multiInsert(rows: Seq[T], connection: Option[Connection]): Int = {
    val result = super.multiInsert(rows, connection)
    if (rows.nonEmpty && result > 0) {
      rows.size
    } else {
      0
    }
  }

  def optimizeTable(forceMerge: Boolean = false): Unit = {
    execute {
      val query = if (forceMerge) {
        s"OPTIMIZE TABLE ${table} FINAL"
      } else {
        s"OPTIMIZE TABLE ${table}"
      }
      executeUpdate(query)(_)
    }
  }

  def optimizePartition(database: String, table: String, partitionId: String, forceMerge: Boolean = false): Unit = {
    execute {
      val query = if (forceMerge) {
        s"OPTIMIZE TABLE `$database`.`${table}` FINAL PARTITION ID '${partitionId}'"
      } else {
        s"OPTIMIZE TABLE `$database`.`${table}` PARTITION ID '${partitionId}'"
      }
      executeUpdate(query)(_)
    }
  }

}
