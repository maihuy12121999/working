package rever.etl.emc.tool.hot_lead_export.reader

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Dataset, Row}
import rever.etl.emc.jobs.common.PersonalContactReaderMixin
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.utils.TimestampUtils

class CreatedPContactReader extends SourceReader with PersonalContactReaderMixin {
  override def read(s: String, config: Config): Dataset[Row] = {
    val (fromTime, toTime) = (
      TimestampUtils.parseMillsFromString(config.get("forcerun.from_date"), "yyyy-MM-dd"),
      TimestampUtils.parseMillsFromString(config.get("forcerun.to_date"), "yyyy-MM-dd")
    )

    val systemTags = config.get("system_tags", "hot_lead")

    val df = readPContactDataset(config, Seq("create"), fromTime, toTime)

    df.withColumn("owner_id", col("owner"))
      .where(col("system_tags").contains(lit(systemTags)))
  }
}
