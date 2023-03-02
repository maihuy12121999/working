package rever.etl.data_sync.core

import org.elasticsearch.search.SearchHit
import rever.etl.data_sync.core.clickhouse.ClickhouseSink
import rever.etl.data_sync.core.es.EsSource
import rever.etl.data_sync.core.mysql.MySqlSource
import rever.etl.data_sync.core.rap.RapIngestionSink
import rever.etl.data_sync.domain.GenericRecord

import java.util.concurrent.atomic.LongAdder

/** @author anhlt (andy)
  * @since 05/07/2022
  */
trait Runner {

  def run(): Long
}

trait Normalizer[T, U] {
  def toRecord(searchHit: T, total: Long): Option[U]
}

case class EsToClickhouseFlow(
    source: EsSource,
    sink: RapIngestionSink,
    normalizer: Normalizer[SearchHit, Map[String, Any]]
) extends Runner {

  override def run(): Long = {
    val counter = new LongAdder()

    source.pull((searchHit, _, totalHits) => {
      normalizer
        .toRecord(searchHit, totalHits)
        .foreach(record => {
          sink.write(record)
          counter.increment()
        })

    })

    sink.end()

    println(s"${getClass.getSimpleName}: Synced ${counter.longValue()} records")

    counter.longValue()
  }

}

case class EsToClickhouseFlow2(
    source: EsSource,
    normalizer: Normalizer[SearchHit, Map[String, Any]],
    sink: ClickhouseSink
) extends Runner {

  override def run(): Long = {
    val totalCounter = new LongAdder()
    val syncedCounter = new LongAdder()

    source.pull((searchHit, _, totalHits) => {
      normalizer
        .toRecord(searchHit, totalHits)
        .foreach(record => {
          sink.write(record)
          totalCounter.reset()
          totalCounter.add(totalHits)
          syncedCounter.increment()
        })
    })

    sink.end()

    println(s"${getClass.getSimpleName}: Synced ${syncedCounter.longValue()}/${totalCounter.longValue()} records")

    syncedCounter.longValue()
  }

}

object MySQLToClickhouseFlow {
  def apply(
      source: MySqlSource,
      normalizer: Normalizer[GenericRecord, Map[String, Any]],
      sink: ClickhouseSink
  ): MySQLToClickhouseFlow = {
    MySQLToClickhouseFlow(source, Map(normalizer -> sink))
  }

  def apply(
      source: MySqlSource,
      sinkMap: Map[Normalizer[GenericRecord, Map[String, Any]], ClickhouseSink]
  ): MySQLToClickhouseFlow = {
    new MySQLToClickhouseFlow(source, sinkMap)
  }
}

class MySQLToClickhouseFlow(
    source: MySqlSource,
    sinkMap: Map[Normalizer[GenericRecord, Map[String, Any]], ClickhouseSink]
) extends Runner {

  override def run(): Long = {
    val totalCounter = new LongAdder()
    val syncedCounter = new LongAdder()

    source.pull((record, _, totalHits) => {

      sinkMap.foreach { case (normalizer, sink) =>
        normalizer
          .toRecord(record, totalHits)
          .foreach(record => {
            sink.write(record)
            totalCounter.reset()
            totalCounter.add(totalHits)
            syncedCounter.increment()

          })
      }

    })

    sinkMap.foreach { case (_, sink) =>
      sink.end()
    }

    println(s"${getClass.getSimpleName}: Synced ${syncedCounter.longValue()}/${totalCounter.longValue()} records")

    syncedCounter.longValue()
  }

}
