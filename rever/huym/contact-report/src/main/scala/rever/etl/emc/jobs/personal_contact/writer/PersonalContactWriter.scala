package rever.etl.emc.jobs.personal_contact.writer

import org.apache.spark.sql.DataFrame
import rever.etl.emc.jobs.personal_contact.PersonalContactSchema
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.extensions.RapIngestWriterMixin

class PersonalContactWriter extends SinkWriter with RapIngestWriterMixin {
  override def write(tableName: String, dataFrame: DataFrame, config: Config): DataFrame = {

    val topic = config.get("total_personal_contact_topic")

    ingestDataframe(config, dataFrame, topic)(PersonalContactSchema.toTotalPersonalContactRecord, 800)

    mergeIfRequired(config, dataFrame, topic)

    dataFrame

  }
}
