package rever.etl.emc.jobs.historical.sys_contact.reader

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row}
import rever.etl.emc.domain.HistoricalSysContactFields
import rever.etl.emc.jobs.common.SysContactReaderMixin
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.domain.SparkSessionImplicits._
class HistoricalSysContactReader extends SourceReader with SysContactReaderMixin {
  override def read(tableName: String, config: Config): Dataset[Row] = {

    readSysContactDataset(config)
      .withColumnRenamed("creator", HistoricalSysContactFields.CREATED_BY)
      .select(
        HistoricalSysContactFields.CID,
        HistoricalSysContactFields.CREATED_BY,
        HistoricalSysContactFields.UPDATED_BY,
        HistoricalSysContactFields.CREATED_TIME,
        HistoricalSysContactFields.UPDATED_TIME,
        HistoricalSysContactFields.PROPERTIES,
        HistoricalSysContactFields.STATUS,
        HistoricalSysContactFields.TIMESTAMP
      )
      .dropDuplicateCols(Seq(HistoricalSysContactFields.CID), col(HistoricalSysContactFields.TIMESTAMP).desc)
      .na
      .fill("")

  }

}
