package rever.etl.emc.jobs.personal_contact.reader

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, Row}
import rever.etl.emc.domain.PersonalContactFields
import rever.etl.emc.jobs.common.PersonalContactReaderMixin
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

class PersonalContactReader extends SourceReader with PersonalContactReaderMixin {
  override def read(tableName: String, config: Config): Dataset[Row] = {
    readDailyPContactDataset(config)
      .orderBy(col("timestamp").desc)
      .dropDuplicates("p_cid")
      .select(
        col("timestamp"),
        col("owner").as(PersonalContactFields.AGENT_ID),
        col("p_cid").as(PersonalContactFields.P_CID),
        col("status").cast(StringType)
      )
  }
}
