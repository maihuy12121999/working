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

  /** Select a list of changed records in the given time range
    * @param from
    * @param size
    * @param fromTime
    * @param toTime
    * @return
    */
  def selectRecordChanged(
      from: Int,
      size: Int,
      fromTime: Long,
      toTime: Long,
      timeField: String
  ): (Long, Seq[GenericRecord]) = {

    execute(ds) { connection =>
      val totalRecord = executeQueryCount(
        s"""
           |SELECT COUNT(*)
           |FROM ${table}
           |WHERE 1=1
           | AND ${timeField} >= ?
           | AND ${timeField} < ?
           |""".stripMargin,
        Seq(fromTime, toTime)
      )(connection)

      val records = totalRecord > 0 match {
        case false => Seq.empty[GenericRecord]
        case true =>
          executeSelectList(
            s"""
               |SELECT *
               |FROM ${table}
               |WHERE 1=1
               | AND ${timeField} >= ?
               | AND ${timeField} < ?
               |ORDER BY ${timeField} ASC
               |LIMIT ${from}, ${size}
               |""".stripMargin,
            Seq(fromTime, toTime)
          )(connection)
      }

      (totalRecord, records)
    }

  }

}
