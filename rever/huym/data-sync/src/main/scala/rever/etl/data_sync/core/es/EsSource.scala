package rever.etl.data_sync.core.es

import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.sort.{SortBuilders, SortOrder}
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
    timeField: String,
    batchSize: Int = 500
) extends Source[SearchHit] {

  private val client = ElasticSearchClient.client(config)

  override def pull(consumeFn: (SearchHit, Long, Long) => Unit): Long = {

    val processedCounter = new LongAdder()

    client.fetchAll2(
      esType,
      query,
      sortBuilders = Seq(SortBuilders.fieldSort(timeField).order(SortOrder.ASC)),
      size = batchSize
    ) { case (searchHit, _, totalHits) =>
      processedCounter.increment()
      consumeFn(searchHit, processedCounter.longValue(), totalHits)
    }

    processedCounter.longValue()
  }
}
