package rever.etl.data_sync.jobs.survey_response.writer

import org.apache.spark.sql.DataFrame
import rever.etl.data_sync.core.clickhouse.ClickhouseSink
import rever.etl.data_sync.domain.survey_response.SurveyResponseDM
import rever.etl.data_sync.jobs.survey_response.EnjoyedSurveyResponseHelper
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config

object EnjoyedResponseSurveyClickHouseSinkFactory {
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

class EnjoyedSurveyResponseWriter extends SinkWriter {

  override def write(tableName: String, df: DataFrame, config: Config): DataFrame = {
    df.rdd.foreachPartition(rows => {
      val sink = EnjoyedResponseSurveyClickHouseSinkFactory.getSink(config)
      val records = rows.map(row => EnjoyedSurveyResponseHelper.toSurveyResponseRecord(row)).toSeq
      records.foreach { record =>
        val insertRecord = sink.write(record)
        if (insertRecord <= 0)
          throw new Exception(s"Cannot write Enjoyed Survey Response to WH")
      }
      sink.end()
    })

    val sink = EnjoyedResponseSurveyClickHouseSinkFactory.getSink(config)
    sink.mergeDuplication()

    df
  }
}
