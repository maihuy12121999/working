package rever.etl.engagement.email.writer

import org.apache.spark.sql.DataFrame
import rever.etl.engagement.email.EmailHelper
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.extensions.RapIngestWriterMixin

class EmailWriter extends SinkWriter with RapIngestWriterMixin {
  override def write(tableName: String, dataFrame: DataFrame, config: Config): DataFrame = {

    val topic = config.get("new_email_engagement_topic")

    ingestDataframe(config, dataFrame, topic)(EmailHelper.toNewEmailRecord, 800)

    mergeIfRequired(config, dataFrame, topic)

    dataFrame
  }
}
