package restore_oppo_historical

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, collect_list}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import restore_oppo_historical.reader.{GSheetMissingOppoHistoryReader, OppoHistoricalReader}
import restore_oppo_historical.writer.MissingOppoHistoricalWriter
import rever.etl.data_sync.core.clickhouse.ClickhouseSink
import rever.etl.data_sync.domain.standardized_oppo_historical.OppoHistoricalRecord
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.utils.JsonUtils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object RestoreOppoHistoricalToCH {
  private var sink: Option[ClickhouseSink] = None

  def getSink(config: Config): ClickhouseSink = {
    val targetTable = config.get("target_table")
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
                OppoHistoricalRecord.PRIMARY_IDS,
                OppoHistoricalRecord.FIELDS,
                targetBatchSize
              )

              sink = Some(clickhouseSink)

              clickhouseSink
          }

        }
    }
  }
}

class RestoreOppoHistoricalToCH extends FlowMixin {

  @Output(writer = classOf[MissingOppoHistoricalWriter])
  @Table("oppo_historical_to_ch")
  def build(
      @Table(
        name = "gsheet_missing_oppo_history_dataset",
        reader = classOf[GSheetMissingOppoHistoryReader]
      ) missingOppoHistoricalDf: DataFrame,
      @Table(
        name = "oppo_historical",
        reader = classOf[OppoHistoricalReader]
      ) actualOppoHistoricalDf: DataFrame,
      config: Config
  ): DataFrame = {

    val df = missingOppoHistoricalDf
      .unionByName(actualOppoHistoricalDf)
      .map(row => {
        val oppoId = row.getAs[String](RestoreOppoHistoricalHelper.OBJECT_ID)
        val data = Option(row.getAs[String](RestoreOppoHistoricalHelper.DATA)).filterNot(_.isEmpty).getOrElse("{}")
        val oldData =
          Option(row.getAs[String](RestoreOppoHistoricalHelper.OLD_DATA)).filterNot(_.isEmpty).getOrElse("{}")
        val detailRow = JsonUtils.toJson(
          Map(
            "action" -> row.getAs[String](RestoreOppoHistoricalHelper.ACTION),
            "data" -> data,
            "old_data" -> oldData,
            "source" -> row.getAs[String](RestoreOppoHistoricalHelper.SOURCE),
            "performer" -> row.getAs[String](RestoreOppoHistoricalHelper.PERFORMER),
            "timestamp" -> row.getAs[Long](RestoreOppoHistoricalHelper.TIMESTAMP),
            "type" -> row.getAs[String](RestoreOppoHistoricalHelper.TYPE)
          )
        )

        Row(oppoId, detailRow)
      })(RowEncoder(RestoreOppoHistoricalHelper.oppoHistoricalRawSchema))
      .toDF(RestoreOppoHistoricalHelper.OBJECT_ID, "detail_row")
      .groupBy(col(RestoreOppoHistoricalHelper.OBJECT_ID))
      .agg(collect_list("detail_row").as("detail_rows"))
      .flatMap(enhanceMissingInfo)(RowEncoder(RestoreOppoHistoricalHelper.oppoHistoricalSchema))

    val restoredOppoHistoricalDf = df
      .select(
        col(RestoreOppoHistoricalHelper.ACTION),
        col(RestoreOppoHistoricalHelper.OBJECT_ID),
        col(RestoreOppoHistoricalHelper.OLD_DATA),
        col(RestoreOppoHistoricalHelper.DATA),
        col(RestoreOppoHistoricalHelper.SOURCE),
        col(RestoreOppoHistoricalHelper.PERFORMER),
        col(RestoreOppoHistoricalHelper.TIMESTAMP)
      )
      .persist(StorageLevel.MEMORY_AND_DISK_2)

    restoredOppoHistoricalDf.show(30, truncate = false)
    restoredOppoHistoricalDf.printSchema()

    println(s"Rows: ${restoredOppoHistoricalDf.count()}")

    restoredOppoHistoricalDf

  }

  private def enhanceMissingInfo(row: Row): Seq[Row] = {
    val rowBuffer: ListBuffer[Row] = ListBuffer.empty
    val objectId = row.getAs[String](RestoreOppoHistoricalHelper.OBJECT_ID)
    var previousOppoHistorical: Option[String] = None

    row
      .getAs[mutable.WrappedArray[String]]("detail_rows")
      .map(toOppoHistory)
      .sortBy(_.timestamp)(Ordering.Long)
      .foreach(history => {
        val timestamp = history.timestamp
        val data = history.newData
        val oldData = history.oldData
        val action = history.action
        val performer = history.performer
        val source = history.source
        val dfType = history.dfType

        dfType match {
          case "oppo_historical" => previousOppoHistorical = Some(JsonUtils.mergeJson(oldData, data).toString)
          case "missing_oppo_history" =>
            val dataRow = mergeJsonRow(
              data,
              previousOppoHistorical,
              objectId,
              timestamp,
              oldData,
              action,
              performer,
              source
            )

            dataRow.foreach(row => {
              rowBuffer.append(row)
            })

          case _ =>
        }
      })

    rowBuffer
  }

  private def toOppoHistory(row: String): OppoHistory = {
    val node = JsonUtils.toJsonNode(row)
    OppoHistory(
      node.at("/timestamp").asLong(0L),
      node.at("/old_data").asText(""),
      node.at("/data").asText(""),
      node.at("/performer").asText(""),
      node.at("/source").asText(""),
      node.at("/action").asText(""),
      node.at("/type").asText("")
    )
  }

  private def mergeJsonRow(
      data: String,
      previousOppoHistorical: Option[String],
      objectId: String,
      timestamp: Long,
      oldData: String,
      action: String,
      performer: String,
      source: String
  ): Option[Row] = {
    previousOppoHistorical match {
      case Some(previousOppoHistorical) =>
        val newDataMissingOppoHistory = JsonUtils.mergeJson(previousOppoHistorical, data).toString
        val oldDataMissingOppoHistory = JsonUtils.mergeJson(previousOppoHistorical, oldData).toString

        Some(
          Row(
            timestamp,
            oldDataMissingOppoHistory,
            newDataMissingOppoHistory,
            performer,
            source,
            action,
            objectId
          )
        )
      case None =>
        Some(
          Row(
            timestamp,
            oldData,
            data,
            performer,
            source,
            action,
            objectId
          )
        )
    }
  }

}
