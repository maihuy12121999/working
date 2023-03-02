package rever.etl.data_sync.core

import org.elasticsearch.search.SearchHit
import rever.etl.data_sync.core.es.EsSource
import rever.etl.data_sync.core.rap.RapIngestionSink

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
