package rever.etl.emc.jobs.historical.personal_contact.reader

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row}
import rever.etl.emc.domain.HistoricalPContactFields
import rever.etl.emc.jobs.common.PersonalContactReaderMixin
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.domain.SparkSessionImplicits._

class HistoricalPersonalContactReader extends SourceReader with PersonalContactReaderMixin {
  override def read(tableName: String, config: Config): Dataset[Row] = {
    readDailyPContactDataset(config)
      .select(
        HistoricalPContactFields.P_CID,
        HistoricalPContactFields.CID,
        HistoricalPContactFields.CREATED_TIME,
        HistoricalPContactFields.UPDATED_TIME,
        HistoricalPContactFields.PROPERTIES,
        HistoricalPContactFields.STATUS,
        HistoricalPContactFields.TIMESTAMP
      )
      .dropDuplicateCols(
        Seq(HistoricalPContactFields.P_CID),
        col(HistoricalPContactFields.TIMESTAMP).desc
      )
      .na
      .fill("")
  }

}
