package rever.etl.data_sync.jobs.transaction

import rever.etl.data_sync.core.Source
import rever.etl.data_sync.core.mysql.MySqlSink
import rever.etl.data_sync.domain.transaction.Opportunity
import rever.etl.data_sync.repository.MySQLOppoDAO
import rever.etl.rsparkflow.api.configuration.Config

import java.util.concurrent.atomic.LongAdder
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt

/** @author anhlt (andy)
  * @since 05/07/2022
  */
case class OppoSource(
    config: Config,
    oppoTable: String,
    isSyncAll: Boolean,
    batchSize: Int = 500
) extends Source[Opportunity] {

  private val ds = MySqlSink.client(config)
  private val oppoDAO = MySQLOppoDAO(ds, oppoTable)

  override def pull(consumeFn: (Opportunity, Long, Long) => Unit): Long = {

    val (fromTime, toTime) = if (isSyncAll) {
      (0L, System.currentTimeMillis())
    } else {
      (config.getHourlyReportTime.longValue(), config.getHourlyReportTime.longValue() + 1.hour.toMillis)
    }

    val counter = new LongAdder()

    val buffer = ListBuffer.empty[Opportunity]
    var from: Int = 0

    do {
      buffer.clear()
      val (totalRecord, oppoRecords) = oppoDAO.selectOppoChanged(from, batchSize, fromTime, toTime)
      buffer.append(oppoRecords: _*)
      buffer.foreach(oppo => {
        counter.increment()
        consumeFn(oppo, counter.longValue(), totalRecord)
      })

      from += batchSize
    } while (buffer.nonEmpty)

    counter.longValue()
  }
}
