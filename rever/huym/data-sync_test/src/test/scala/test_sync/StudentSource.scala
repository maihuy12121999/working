package test_sync

import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import rever.etl.data_sync.core.Source
import rever.etl.data_sync.core.mysql.MySqlSink
import rever.etl.rsparkflow.api.configuration.Config

import java.util.concurrent.atomic.LongAdder
import scala.collection.mutable.ListBuffer

case class StudentSource(
    config: Config,
    studentTable:String,
    isSyncAll:Boolean,
    batchSize:Int=500
) extends Source[StudentRecord]{
  private val ds = MySqlSink.client(config)
  private val studentDao = MySqlStudentDAO(ds,studentTable)

  override def pull(consumeFn: (StudentRecord, Long, Long) => Unit): Long = {
    val (fromTime,toTime) = if (isSyncAll){
      (0L,System.currentTimeMillis())
    }else{
      (config.getHourlyReportTime.longValue(),config.getHourlyReportTime.longValue()+1.hour.toMillis)
    }

    val counter = new LongAdder()
    val buffer = ListBuffer.empty[StudentRecord]
    var from = 0
    do{
      buffer.clear()
      val (totalRecord, studentRecords) = studentDao.selectStudentChanged(from,batchSize,fromTime,toTime)
      buffer.append(studentRecords: _*)
      buffer.foreach(
        student=> {
          counter.increment()
          consumeFn(student,counter.longValue(),totalRecord)
        }
      )
    }
    while(buffer.nonEmpty)

    counter.longValue()
  }
}
