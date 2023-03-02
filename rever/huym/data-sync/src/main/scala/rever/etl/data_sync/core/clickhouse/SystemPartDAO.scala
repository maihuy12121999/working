package rever.etl.data_sync.core.clickhouse

import rever.etl.data_sync.domain.GenericRecord

import javax.sql.DataSource

case class SystemPartDAO(ds: DataSource, tableName: String) extends CustomChDAO[GenericRecord] {
  override val table: String = tableName

  override def createRecord(): GenericRecord = GenericRecord(
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
          |select count(*) part_count,
          |sum(data_uncompressed_bytes) as data_uncompressed_bytes,
          |formatReadableSize(data_uncompressed_bytes) total_readable_size,
          |database,table,partition , partition_id
          |from system.parts
          |where active
          |AND database IN (${optimizedDatabases.map(_ => "?").mkString(", ")})
          |group by database,table,partition, partition_id
          |HAVING part_count > 1 AND if(${maxSizeInBytes}>0, data_uncompressed_bytes <= ${maxSizeInBytes}, 1)
          |order by  data_uncompressed_bytes ASC, part_count desc
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
