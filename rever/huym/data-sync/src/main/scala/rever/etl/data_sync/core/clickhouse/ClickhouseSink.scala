package rever.etl.data_sync.core.clickhouse

import com.zaxxer.hikari.HikariDataSource
import rever.etl.data_sync.core.Sink
import rever.etl.data_sync.domain.GenericRecord
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.utils.TimestampUtils
import vn.rever.jdbc.clickhouse.ClickHouseEngine

import java.util.concurrent.atomic.LongAdder
import javax.sql.DataSource
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt

object ClickhouseSink {
  private val dsMap = scala.collection.mutable.Map.empty[String, DataSource]

  def client(config: Config): DataSource = {
    val host = config.get("CH_HOST")
    val port = config.getInt("CH_PORT")
    val db = config.get("CH_DB")

    val cacheKey = s"${host}_${port}_${db}"

    dsMap.get(cacheKey) match {
      case Some(client) => client
      case None =>
        dsMap.synchronized {
          dsMap
            .get(cacheKey)
            .fold[DataSource]({
              val ds = getDataSource(config)
              dsMap.put(cacheKey, ds)
              ds
            })(x => x)

        }
    }
  }

  private def getDataSource(config: Config): HikariDataSource = {
    val db = config.get("CH_DB")
    val ds = ClickHouseEngine
      .getDataSource(
        host = config.get("CH_HOST"),
        port = config.getInt("CH_PORT"),
        user = config.get("CH_USER_NAME"),
        password = config.get("CH_PASSWORD"),
        dbName = db
      )
      .asInstanceOf[HikariDataSource]

    val compress = config.getBoolean("CH_COMPRESS", false)
    val timeoutMs = config.getLong("CH_TIMEOUT_MS", 30000L)

    ds.addDataSourceProperty("compress", compress)
    ds.addDataSourceProperty("decompress", compress)
    ds.addDataSourceProperty("socket_timeout", timeoutMs)
    ds.addDataSourceProperty("connection_timeout", timeoutMs)
    ds.setValidationTimeout(java.util.concurrent.TimeUnit.SECONDS.toMillis(60))
    ds
  }
}

case class ClickhouseSink(
    config: Config,
    tableName: String,
    primaryKeys: Seq[String],
    fields: Seq[String],
    batchSize: Int
) extends Sink[Map[String, Any]] {

  private val genericRecordDAO = CHGenericRecordDAO(
    ClickhouseSink.client(config),
    tableName,
    primaryKeys,
    fields
  )

  private val buffer = ListBuffer.empty[Map[String, Any]]

  private val totalCounter = new LongAdder()
  private val syncedCounter = new LongAdder()

  override def write(record: Map[String, Any]): Int = {
    synchronized {
      buffer.append(record)

      if (buffer.size >= batchSize) {
        val count = flushBatch(buffer)
        buffer.clear()
        count
      } else {
        1
      }
    }
  }

  override def writeBatch(records: Seq[Map[String, Any]]): Int = {
    records.foreach(write)

    records.size
  }

  private def flushBatch(records: Seq[Map[String, Any]]): Int = {
    val chRecords = records.map(record => {
      GenericRecord(primaryKeys, fields, record)
    })

    val count = genericRecordDAO.multiInsert(chRecords)
    if (count <= 0 && records.nonEmpty) {
      throw new Exception(s"Failed to ingest data: ${tableName} - ${count}/${records.size}")
    }
    totalCounter.add(records.size)
    syncedCounter.add(count)
    println(s"Sink consumed: ${tableName} - ${records.size}/${syncedCounter.longValue()}/${totalCounter.longValue()}")

    count
  }

  override def end(): Unit = {
    synchronized {
      if (buffer.nonEmpty) {
        flushBatch(buffer)
        buffer.clear()
      }
    }
  }

  final def mergeDuplication(): Unit = {

    val totalSynced = syncedCounter.longValue()
    val mergeAfterWrite = config.getBoolean("merge_after_write", false)

    if (mergeAfterWrite && totalSynced > 0) {
      println("[Start] optimize merge data")
      genericRecordDAO.optimizeTable()
      println("[Completed] optimize merge data")
    }
  }

  final def mergeDuplicationHourly(): Unit = {

    val totalSynced = syncedCounter.longValue()
    val mergeAfterWrite = config.getBoolean("merge_after_write", false)

    val reportHour = TimestampUtils.asStartOfHour(config.getHourlyReportTime)

    val lastReportHour = TimestampUtils.asStartOfDay(config.getHourlyReportTime) + 23.hour.toMillis

    println(s"""
               |Report time: ${TimestampUtils.format(reportHour, Some("yyyy/MM/dd HH:mm:ss"))}
               |Last hour of day: ${TimestampUtils.format(lastReportHour, Some("yyyy/MM/dd HH:mm:ss"))}
               |Total records synced: ${totalSynced}
               |Merge after write: ${mergeAfterWrite}
               |""".stripMargin)

    if (totalSynced > 0) {
      println("[Start] optimize merge data")
      genericRecordDAO.optimizeTable()
      println("[Completed] optimize merge data")
    }
  }

  override def close(): Unit = {
    genericRecordDAO.ds match {
      case ds: HikariDataSource =>
        ds.close()
      case _ =>
    }
  }
}
