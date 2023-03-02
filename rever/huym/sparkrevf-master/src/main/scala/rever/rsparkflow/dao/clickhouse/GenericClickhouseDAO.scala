package rever.rsparkflow.dao.clickhouse

import rever.rsparkflow.dao.GenericRecord

import javax.sql.DataSource

case class GenericClickhouseDAO(
    ds: DataSource,
    table: String,
    primaryKeys: Seq[String],
    fields: Seq[String]
) extends CustomClickhouseDAO[GenericRecord] {

  override def createRecord(): GenericRecord = GenericRecord(primaryKeys, fields)

}
