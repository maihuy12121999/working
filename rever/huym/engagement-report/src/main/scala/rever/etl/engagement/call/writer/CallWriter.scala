package rever.etl.engagement.call.writer

import org.apache.spark.sql.DataFrame
import rever.etl.engagement.call.CallHelper
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.extensions.RapIngestWriterMixin

class CallWriter extends SinkWriter with RapIngestWriterMixin {
  override def write(tableName: String, dataFrame: DataFrame, config: Config): DataFrame = {

    val topic = config.get("new_call_engagement_topic")

    ingestDataframe(config, dataFrame, topic)(CallHelper.toNewCallRecord, 800)
    mergeIfRequired(config, dataFrame, topic)

    dataFrame
  }
}
