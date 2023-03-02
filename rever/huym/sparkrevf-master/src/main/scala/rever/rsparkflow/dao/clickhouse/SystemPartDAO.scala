package rever.rsparkflow.dao.clickhouse

import rever.rsparkflow.dao.GenericRecord

import javax.sql.DataSource

case class SystemPartDAO(ds: DataSource) extends CustomClickhouseDAO[GenericRecord] {
  override val table: String = "system.parts"

  override def createRecord(): GenericRecord =
    GenericRecord(
      Seq("partition_id"),
      Seq("partition_id", "database", "table", "partition", "data_uncompressed_bytes", "part_count")
    )

  def selectPartitionRequiredToMerge(
      optimizedDatabases: Seq[String],
      maxSizeInBytes: Long
  ): Seq[(String, String, String)] = {
    execute { con =>
      val records = executeSelectList(
        s"""
          |SELECT 
          | count(*) part_count,
          | sum(data_uncompressed_bytes) as data_uncompressed_bytes,
          | formatReadableSize(data_uncompressed_bytes) total_readable_size,
          | database,table,
          | partition,
          | partition_id
          |FROM system.parts
          |WHERE active
          | AND database IN (${optimizedDatabases.map(_ => "?").mkString(", ")})
          |GROUP BY database,table,partition, partition_id
          |HAVING part_count > 1 AND if(${maxSizeInBytes}>0, data_uncompressed_bytes <= ${maxSizeInBytes}, 1)
          |ORDER BY data_uncompressed_bytes ASC, part_count desc
          |""".stripMargin,
        optimizedDatabases
      )(con)

      records
        .map(record => {
          val database = record.getValue("database").map(_.toString).getOrElse("")
          val table = record.getValue("table").map(_.toString).getOrElse("")
          val partitionId = record.getValue("partition_id").map(_.toString).getOrElse("")

          (database, table, partitionId)

        })
    }
  }
}
