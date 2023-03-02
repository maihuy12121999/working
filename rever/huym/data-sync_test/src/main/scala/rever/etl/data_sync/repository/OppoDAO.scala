package rever.etl.data_sync.repository

import rever.etl.data_sync.domain.transaction.Opportunity
import vn.rever.jdbc.mysql.MySqlDAO

import javax.sql.DataSource

case class MySQLOppoDAO(ds: DataSource, table: String) extends MySqlDAO[Opportunity] {

  override def createRecord(): Opportunity = Opportunity()

  def selectOppoChanged(from: Int, size: Int, fromTime: Long, toTime: Long): (Long, Seq[Opportunity]) = {

    execute(ds) { connection =>
      val totalRecord = executeQueryCount(
        s"""
              |SELECT COUNT(${Opportunity.ID})
              |FROM ${table}
              |WHERE 1=1
              | AND ${Opportunity.UPDATED_TIME} >= ?
              | AND ${Opportunity.UPDATED_TIME} <= ?
              |""".stripMargin,
        Seq(fromTime, toTime)
      )(connection)

      val records = totalRecord > 0 match {
        case false => Seq.empty[Opportunity]
        case true =>
          executeSelectList(
            s"""
                 |SELECT *
                 |FROM ${table}
                 |WHERE 1=1
                 | AND ${Opportunity.UPDATED_TIME} >= ?
                 | AND ${Opportunity.UPDATED_TIME} <= ?
                 |LIMIT ${from}, ${size}
                 |""".stripMargin,
            Seq(fromTime, toTime)
          )(connection)
      }

      (totalRecord, records)
    }

  }

}
