package rever.etl.data_sync.core.es

import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.search.SearchHit
import rever.etl.data_sync.core.Source
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.client.ElasticSearchClient

import java.util.concurrent.atomic.LongAdder

/** @author anhlt (andy)
  * @since 05/07/2022
  */
case class EsSource(
    config: Config,
    esType: String,
    query: QueryBuilder,
    batchSize: Int = 500
) extends Source[SearchHit] {

  private val client = ElasticSearchClient.client(config)

  override def pull(consumeFn: (SearchHit, Long, Long) => Unit): Long = {

    val counter = new LongAdder()

    client.fetchAll2(
      esType,
      query,
      size = batchSize
    ) { case (searchHit, totalCurrentPageHits, totalHits) =>
      counter.increment()
      consumeFn(searchHit, counter.longValue(), totalHits)
    }

    counter.longValue()
  }
}
