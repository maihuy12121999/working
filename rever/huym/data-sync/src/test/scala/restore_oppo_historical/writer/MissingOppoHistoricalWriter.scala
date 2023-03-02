package restore_oppo_historical.writer

import org.apache.spark.sql.{Dataset, Row}
import restore_oppo_historical.RestoreOppoHistoricalHelper._
import restore_oppo_historical.RestoreOppoHistoricalToCH
import rever.etl.data_sync.domain.standardized_oppo_historical.OppoHistoricalRecord
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config

import java.util.UUID

class MissingOppoHistoricalWriter extends SinkWriter {

  override def write(tableName: String, df: Dataset[Row], config: Config): Dataset[Row] = {
    df.rdd.foreachPartition(rows => {
      val sink = RestoreOppoHistoricalToCH.getSink(config)
      val records = rows.map(toOppoHistoricalRecord).toSeq
      records.foreach { record =>
        val count = sink.write(record)
        if (count <= 0)
          throw new Exception(s"Cannot write Oppo Historical")

      }
      sink.end()

    })

    df
  }

  private def toOppoHistoricalRecord(row: Row): Map[String, Any] = {
    Map[String, Any](
      OppoHistoricalRecord.LOG_TIME -> row.getAs[Long](TIMESTAMP),
      OppoHistoricalRecord.ID -> UUID.randomUUID().toString,
      OppoHistoricalRecord.ACTION -> row.getAs[String](ACTION),
      OppoHistoricalRecord.OBJECT_ID -> row.getAs[String](OBJECT_ID),
      OppoHistoricalRecord.OLD_DATA -> row.getAs[String](OLD_DATA),
      OppoHistoricalRecord.DATA -> row.getAs[String](DATA),
      OppoHistoricalRecord.SOURCE -> row.getAs[String](SOURCE),
      OppoHistoricalRecord.PERFORMER -> row.getAs[String](PERFORMER),
      OppoHistoricalRecord.TIMESTAMP -> row.getAs[Long](TIMESTAMP)
    )
  }
}
