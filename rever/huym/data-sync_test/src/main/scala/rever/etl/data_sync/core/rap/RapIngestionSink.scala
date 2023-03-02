package rever.etl.data_sync.core.rap

import rever.etl.data_sync.core.Sink
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.client.RapIngestionClient
import rever.etl.rsparkflow.utils.{JsonUtils, TimestampUtils}

import java.util.concurrent.atomic.LongAdder
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt

case class RapIngestionSink(topic: String, batchSize: Int, config: Config) extends Sink[Map[String, Any]] {

  private val ingestClient = RapIngestionClient.client(config)
  private val counter = new LongAdder()
  private val buffer = ListBuffer.empty[Map[String, Any]]

  override def write(record: Map[String, Any]): Int = {
    buffer.append(record)

    if (buffer.size >= batchSize) {
      val count = flushBatch(buffer)
      buffer.clear()
      count
    } else {
      1
    }
  }

  override def writeBatch(records: Seq[Map[String, Any]]): Int = {
    records.foreach(write)

    records.size
  }

  private def flushBatch(records: Seq[Map[String, Any]]): Int = {
    val count = ingestClient.ingest(topic, records.map(record => JsonUtils.toJsonNode(JsonUtils.toJson(record))))
    if (count <= 0 && records.nonEmpty) {
      throw new Exception(s"Failed to ingest data: ${topic} - ${count}/${records.size}")
    }
    counter.add(records.size)

    println(s"Sink consumed: ${topic} - ${records.size}/${counter.longValue()}")

    records.size
  }

  override def end(): Unit = {
    if (buffer.nonEmpty) {
      writeBatch(buffer)
      buffer.clear()
    }
  }

  def forceMergeHourlySync(): Unit = {

    val totalSynced = counter.longValue()
    val mergeAfterWrite = config.getBoolean("merge_after_write", false)

    val reportHour = TimestampUtils.asStartOfHour(config.getHourlyReportTime)
    val prevHour = TimestampUtils.asStartOfHour(System.currentTimeMillis() - 1.hour.toMillis)
    val lastHourOfDay = TimestampUtils.asStartOfDay(System.currentTimeMillis()) + 23.hour.toMillis

    if (mergeAfterWrite && totalSynced > 0 && reportHour >= prevHour && prevHour >= lastHourOfDay) {
      println("Starting force merge data")
      RapIngestionClient.client(config).forceMerge(topic)
      println("Force merge data")
    }
  }

  def forceMergeDailySync(): Unit = {

    val totalSynced = counter.longValue()
    val mergeAfterWrite = config.getBoolean("merge_after_write", false)

    val isPresentOrFuture = TimestampUtils.asStartOfDay(config.getDailyReportTime) >= TimestampUtils.asStartOfDay(
      System.currentTimeMillis() - 1.days.toMillis
    )
    if (mergeAfterWrite && isPresentOrFuture && totalSynced > 0) {
      RapIngestionClient.client(config).forceMerge(topic)
    }
  }

}
