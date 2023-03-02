package rever.etl.data_sync.core.mysql

import rever.etl.data_sync.domain.GenericRecord
import vn.rever.jdbc.mysql.MySqlDAO

import javax.sql.DataSource

case class MySQLGenericRecordDAO(
    ds: DataSource,
    tableName: String,
    primaryKeys: Seq[String],
    fields: Seq[String]
) extends MySqlDAO[GenericRecord] {

  override val table: String = tableName

  override def createRecord(): GenericRecord = GenericRecord(primaryKeys, fields)

}
