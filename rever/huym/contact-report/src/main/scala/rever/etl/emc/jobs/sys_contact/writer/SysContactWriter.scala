package rever.etl.emc.jobs.sys_contact.writer

import org.apache.spark.sql.DataFrame
import rever.etl.emc.jobs.sys_contact.SysContactSchema
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.extensions.RapIngestWriterMixin

class SysContactWriter extends SinkWriter with RapIngestWriterMixin {

  override def write(tableName: String, dataFrame: DataFrame, config: Config): DataFrame = {

    val topic = config.get("total_sys_contact_topic")

    ingestDataframe(config, dataFrame, topic)(SysContactSchema.toTotalSysContactRecord, 800)

    mergeIfRequired(config, dataFrame, topic)

    dataFrame

  }

}
