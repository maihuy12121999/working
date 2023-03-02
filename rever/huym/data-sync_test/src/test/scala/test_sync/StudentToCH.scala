package test_sync

import rever.etl.data_sync.core.Runner
import rever.etl.data_sync.core.clickhouse.ClickhouseSink
import rever.etl.rsparkflow.api.configuration.Config

import java.util.concurrent.atomic.LongAdder

case class StudentToCH (config: Config) extends Runner{
  private val sourceStudentTable = config.get("source_student_table")
  private val targetStudentTable = config.get("target_student_table")
  private val isSyncAll = config.getBoolean("is_sync_all",false)

  override def run(): Long = {
    val source = StudentSource(config,sourceStudentTable,isSyncAll)
    val sink = ClickhouseSink(config,targetStudentTable,StudentRecord.primaryKeys,StudentRecord.fields,500)
    val normalizer = StudentNormalizer()
    syncStudentToCH(source,sink,normalizer)
  }
  private def syncStudentToCH(source: StudentSource, sink: ClickhouseSink, normalizer: StudentNormalizer): Long = {
    val counter = new LongAdder()

    source.pull((student,_,totalRecord)=>{
      normalizer
        .toRecord(student,totalRecord)
        .foreach(
          student=>{
            sink.write(student)
            counter.increment()
          }
        )
    })

    sink.end()
    println(s"${getClass.getSimpleName}: Synced $counter records")
    counter.longValue()
  }
}
