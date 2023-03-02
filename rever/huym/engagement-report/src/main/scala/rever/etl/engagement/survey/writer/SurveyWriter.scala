package rever.etl.engagement.survey.writer

import org.apache.spark.sql.DataFrame
import rever.etl.engagement.survey.SurveyHelper
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.extensions.RapIngestWriterMixin

class SurveyWriter extends SinkWriter with RapIngestWriterMixin {
  override def write(tableName: String, dataFrame: DataFrame, config: Config): DataFrame = {

    val topic = config.get("new_survey_engagement_topic")

    ingestDataframe(config, dataFrame, topic)(SurveyHelper.toNewSurveyRecord, 800)

    mergeIfRequired(config, dataFrame, topic)
    dataFrame
  }
}
