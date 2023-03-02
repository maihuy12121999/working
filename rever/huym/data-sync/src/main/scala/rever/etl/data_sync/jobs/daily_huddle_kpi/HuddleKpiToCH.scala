package rever.etl.data_sync.jobs.daily_huddle_kpi

import org.apache.spark.sql.DataFrame
import rever.etl.data_sync.core.clickhouse.ClickhouseSink
import rever.etl.data_sync.domain.HuddleKpiRecord
import rever.etl.rsparkflow.api.annotation.Table
import rever.etl.rsparkflow.api.configuration.Config

object DailyHuddleClickHouseSinkFactory {
  private var sink: Option[ClickhouseSink] = None

  def getSink(config: Config): ClickhouseSink = {
    val targetTable = config.get("target_table", "rever_daily_huddle_kpi")
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
                HuddleKpiRecord.PRIMARY_IDS,
                HuddleKpiRecord.FIELDS,
                batchSize = targetBatchSize
              )

              sink = Some(clickhouseSink)

              clickhouseSink

          }
        }
    }
  }
}

class HuddleKpiToCH {

  @Table("huddle_kpi")
  def build(
      @Table(
        name = "gsheet_daily_huddle_kpi_dataset",
        reader = classOf[GSheetDailyHuddleKpiReader]
      ) huddleKpiDf: DataFrame,
      config: Config
  ): DataFrame = {

    huddleKpiDf.rdd.foreachPartition(rows => {
      val sink = DailyHuddleClickHouseSinkFactory.getSink(config)
      val records = rows.map(row => HuddleKpiHelper.toHuddleKpiRecord(row)).toSeq
      records.foreach { record =>
        val insertRecord = sink.write(record)
        if (insertRecord <= 0)
          throw new Exception(s"Cannot write Daily Huddle KPI to WH")
      }
      sink.end()
    })

    val sink = DailyHuddleClickHouseSinkFactory.getSink(config)
    sink.mergeDuplication()

    huddleKpiDf
  }

}
