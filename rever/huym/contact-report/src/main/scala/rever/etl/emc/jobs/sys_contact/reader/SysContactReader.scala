package rever.etl.emc.jobs.sys_contact.reader

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row}
import rever.etl.emc.domain.SysContactFields
import rever.etl.emc.jobs.common.SysContactReaderMixin
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

/** @author anhlt
  */
class SysContactReader extends SourceReader with SysContactReaderMixin {

  override def read(s: String, config: Config): Dataset[Row] = {
    readSysContactDataset(config)
      .orderBy(col("timestamp").desc)
      .dropDuplicates("cid")
      .select(
        col("timestamp"),
        col("creator").as(SysContactFields.AGENT_ID),
        col("cid").as(SysContactFields.CID),
        col("status")
      )
  }

}
