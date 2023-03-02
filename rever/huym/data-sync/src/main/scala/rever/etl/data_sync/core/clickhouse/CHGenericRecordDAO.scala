package rever.etl.data_sync.core.clickhouse

import rever.etl.data_sync.domain.GenericRecord

import javax.sql.DataSource

case class CHGenericRecordDAO(
    ds: DataSource,
    tableName: String,
    primaryKeys: Seq[String],
    fields: Seq[String]
) extends CustomChDAO[GenericRecord] {

  override val table: String = tableName

  override def createRecord(): GenericRecord = GenericRecord(primaryKeys, fields)

}
