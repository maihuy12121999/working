package rever.etl.data_sync.jobs.delighted_survey_response.writer

import org.apache.spark.sql.DataFrame
import rever.etl.data_sync.core.clickhouse.ClickhouseSink
import rever.etl.data_sync.domain.survey_response.SurveyResponseDM
import rever.etl.data_sync.jobs.delighted_survey_response.DelightedSurveyResponseHelper
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config

object DelightedResponseSurveyClickHouseSinkFactory {
  private var sink: Option[ClickhouseSink] = None

  def getSink(config: Config): ClickhouseSink = {
    val targetTable = config.get("target_table", "rever_survey_response")
    val targetBatchSize = config.getInt("target_batch_size", 500)

    sink match {
      case Some(sink) => sink
      case None =>
        synchronized {
          sink match {
            case Some(sink) => sink
            case None =>
              val clickhouseSink = new ClickhouseSink(
                config,
                targetTable,
                SurveyResponseDM.PRIMARY_IDS,
                SurveyResponseDM.FIELDS,
                batchSize = targetBatchSize
              )

              sink = Some(clickhouseSink)

              clickhouseSink

          }
        }
    }
  }
}

class DelightedSurveyResponseWriter extends SinkWriter {
  override def write(tableName: String, df: DataFrame, config: Config): DataFrame = {
    df.rdd.foreachPartition(rows => {
      val sink = DelightedResponseSurveyClickHouseSinkFactory.getSink(config)
      val records = rows.map(row => DelightedSurveyResponseHelper.toSurveyResponseRecord(row)).toSeq
      records.foreach { record =>
        val insertRecord = sink.write(record)
        if (insertRecord <= 0)
          throw new Exception(s"Cannot write Delighted Survey Response to WH")
      }
      sink.end()
    })

    val sink = DelightedResponseSurveyClickHouseSinkFactory.getSink(config)
    sink.mergeDuplication()

    df
  }
}
