package rever.etl.data_sync.core.mysql

import rever.etl.data_sync.core.Source
import rever.etl.data_sync.domain.GenericRecord
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.utils.TimestampUtils

import java.util.concurrent.atomic.LongAdder
import scala.collection.mutable.ListBuffer

/** @author anhlt (andy)
  * @since 05/07/2022
  */
case class MySqlSource(
    config: Config,
    sourceTable: String,
    primaryKeys: Seq[String],
    fields: Seq[String],
    timeField: String,
    fromTime: Long,
    toTime: Long,
    batchSize: Int = 200
) extends Source[GenericRecord] {

  private val ds = MySqlSink.client(config)
  private val genericRecordDAO = MySQLGenericRecordDAO(ds, sourceTable, primaryKeys, fields)

  override def pull(consumeFn: (GenericRecord, Long, Long) => Unit): Long = {
    println(s"Going to sync data from: ${TimestampUtils.format(fromTime)} ->  ${TimestampUtils.format(toTime)}")

    val counter = new LongAdder()

    val buffer = ListBuffer.empty[GenericRecord]
    var from: Int = 0

    do {
      buffer.clear()
      val (totalRecord, genericRecords) = genericRecordDAO.selectRecordChanged(
        from,
        batchSize,
        fromTime,
        toTime,
        timeField
      )
      buffer.append(genericRecords: _*)
      buffer.foreach(record => {
        counter.increment()
        consumeFn(record, counter.longValue(), totalRecord)
      })

      from += batchSize
    } while (buffer.nonEmpty)

    counter.longValue()
  }

}
