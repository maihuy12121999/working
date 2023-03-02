package rever.etl.support.rever_academy.writer

import org.apache.spark.sql.DataFrame
import rever.rsparkflow.spark.api.SinkWriter
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.extensions.RapIngestWriterMixin
import rever.rsparkflow.spark.utils.JsonUtils

class AcademyStudentWriter extends SinkWriter with RapIngestWriterMixin {
  override def write(tableName: String, dataFrame: DataFrame, config: Config): DataFrame = {
    val topic = config.get("rever_academy_student_topic")

    ingestDataframe(config, dataFrame, topic)(
      row => {
        val jsonRow = JsonUtils.toJson(
          Map(
            "course" -> row.getAs[String]("course"),
            "username" -> row.getAs[String]("username"),
            "email" -> row.getAs[String]("email"),
            "name" -> row.getAs[String]("name")
          )
        )
        JsonUtils.toJsonNode(jsonRow)
      },
      batchSize = 800
    )

    mergeIfRequired(config, dataFrame, topic)
    dataFrame
  }

}
