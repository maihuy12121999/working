package rever.rsparkflow.spark.client

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.delete.{DeleteRequest, DeleteResponse}
import org.elasticsearch.action.get.{GetResponse, MultiGetItemResponse, MultiGetRequest, MultiGetResponse}
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse}
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.VersionType
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.fetch.source.FetchSourceContext
import org.elasticsearch.search.sort.SortBuilder
import rever.rsparkflow.spark.api.configuration.Config

import java.net.InetAddress
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Try

/**
  * @author anhlt (andy)
  * @since 04/07/2022
 **/

trait BaseElasticSearchClient {

  val MAX_RETRY_SCROLL = 5

  protected val client: TransportClient

  val indexName: String

  def getClient = client

  def getIndexName = indexName

  def putOrMergeSettingAndMapping(setting: String, mappings: Seq[(String, String)] = Seq()): Unit = {
    val existIndex = client.admin().indices().prepareExists(indexName).get().isExists
    if (!existIndex) {
      val prepareIndex = client.admin().indices().prepareCreate(indexName)
      if (setting.nonEmpty) prepareIndex.setSettings(setting)
      mappings.foreach(f => prepareIndex.addMapping(f._1, f._2))
      prepareIndex.get()
    } else {
      mappings.foreach(f => updateMappingSync(f._1, f._2))
    }
  }

  def createIndexWithSettingAndMapping(setting: String, mappings: Seq[(String, String)] = Seq()): Unit = {
    if (!client.admin().indices().prepareExists(indexName).get().isExists) {
      val prepareIndex = client.admin().indices().prepareCreate(indexName)
      if (setting.nonEmpty) prepareIndex.setSettings(setting)
      mappings.foreach(f => prepareIndex.addMapping(f._1, f._2))
      prepareIndex.get()
    }
  }

  def updateMappingSync(types: String, mapping: String): PutMappingResponse = {
    client
      .admin()
      .indices()
      .preparePutMapping(indexName)
      .setType(types)
      .setSource(mapping)
      .get()

  }

  def getSync(
      types: String,
      id: String,
      includeFields: Array[String] = null,
      excludeFields: Array[String] = null
  ): GetResponse = {
    val req = client.prepareGet(indexName, types, id)
    if (includeFields != null || excludeFields != null) {
      req.setFetchSource(includeFields, excludeFields)
    }
    req.execute().actionGet()
  }

  def mgetSync(
      types: String,
      listId: Seq[String],
      includeFields: Array[String] = null,
      excludeFields: Array[String] = null
  ): MultiGetResponse = {
    if (listId.isEmpty) new MultiGetResponse(Array.empty[MultiGetItemResponse])
    else {
      val req = client.prepareMultiGet()
      if (includeFields != null || excludeFields != null) {
        val fetchSourceContext = new FetchSourceContext(includeFields, excludeFields)
        listId.foreach(id => {
          req.add(new MultiGetRequest.Item(indexName, types, id).fetchSourceContext(fetchSourceContext))
        })
      } else {
        req.add(indexName, types, listId.asJava)
      }
      req.execute().actionGet()
    }
  }

  def mgetByTypesSync(
      typeWithIds: Map[String, Seq[String]],
      includeFields: Array[String] = null,
      excludeFields: Array[String] = null
  ): MultiGetResponse = {
    if (typeWithIds.isEmpty) new MultiGetResponse(Array.empty[MultiGetItemResponse])
    else {
      val req = client.prepareMultiGet()
      if (includeFields != null || excludeFields != null) {
        val fetchSourceContext = new FetchSourceContext(includeFields, excludeFields)
        typeWithIds.foreach(typeWithId => {
          typeWithId._2.foreach(id => {
            req.add(new MultiGetRequest.Item(indexName, typeWithId._1, id).fetchSourceContext(fetchSourceContext))
          })
        })
      } else {
        typeWithIds.foreach(typeWithId => {
          req.add(indexName, typeWithId._1, typeWithId._2.asJava)
        })
      }
      req.execute().get()
    }
  }

  def existSync(types: String, id: String): Boolean = {
    client
      .prepareGet(indexName, types, id)
      .setFetchSource(false)
      .get()
      .isExists
  }

  def mExistSync(types: String, listId: Seq[String]): Map[String, Boolean] = {
    if (listId.isEmpty) Map()
    else {
      val req = client.prepareMultiGet()
      listId.foreach(id => {
        req.add(
          new MultiGetRequest.Item(indexName, types, id).fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE)
        )
      })
      req.get().getResponses.map(resp => resp.getId -> (if (resp.isFailed) false else resp.getResponse.isExists)).toMap
    }
  }

  def mExistSync(mapTypeIds: Map[String, Seq[String]]): Map[String, Map[String, Boolean]] = {
    val mapIds = mapTypeIds.filter(_._2.nonEmpty)
    if (mapIds.isEmpty) Map()
    else {
      val mgetReq = client.prepareMultiGet()
      mapIds.foreach(typeIds =>
        typeIds._2.foreach(id => {
          mgetReq.add(
            new MultiGetRequest.Item(indexName, typeIds._1, id)
              .fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE)
          )
        })
      )
      val mResp = mgetReq.get()
      val map = mapTypeIds.map(f => f._1 -> scala.collection.mutable.Map.empty[String, Boolean])
      mResp.getResponses.foreach(resp =>
        map(resp.getType) += resp.getId -> (if (resp.isFailed) false else resp.getResponse.isExists)
      )
      map.map(f => f._1 -> f._2.toMap)
    }
  }

  def exist2Sync(types: String, queryBuilders: QueryBuilder): Boolean = {
    client
      .prepareSearch(indexName)
      .setTypes(types)
      .setFetchSource(false)
      .setSize(1)
      .setQuery(queryBuilders)
      .get()
      .getHits
      .getTotalHits > 0
  }

  def prepareSearch = client.prepareSearch(indexName)

  def prepareSearchMore10k(
      enhanceSearchReqBuilder: SearchRequestBuilder => SearchRequestBuilder,
      from: Int,
      size: Int
  ): SearchResponse = {
    val searchReq = enhanceSearchReqBuilder(client.prepareSearch(indexName))
    var scrollFrom: Int = size

    var hasNext: Boolean = true
    var resp: SearchResponse = searchReq.setSize(size).setScroll(new TimeValue(2000L)).get()
    while (scrollFrom < from + size && hasNext) {
      if (resp.getHits.getTotalHits == 0) hasNext = false
      else {
        resp = client.prepareSearchScroll(resp.getScrollId).setScroll(new TimeValue(2000L)).get()
        scrollFrom += size
      }
    }
    resp
  }

  def prepareCount = client.prepareCount(indexName)

  def prepareIndex(typeName: String) = client.prepareIndex(indexName, typeName)

  def prepareUpdate(typeName: String, id: String) = client.prepareUpdate(indexName, typeName, id)

  def fetchAll(
      types: String,
      queryBuilders: QueryBuilder,
      size: Int = 50,
      scrollKeepAliveInMs: Long = 60000L,
      fetchSource: Boolean = true,
      includeFields: Array[String] = null,
      excludeFields: Array[String] = null,
      limitTotalFetch: Int = 0
  )(f: SearchHit => Unit): Unit = {

    var req = client
      .prepareSearch(indexName)
      .setTypes(types)
      .setSize(size)
      .setQuery(queryBuilders)
      .setScroll(new TimeValue(scrollKeepAliveInMs))

    if (!fetchSource) req.setFetchSource(fetchSource)
    else if (includeFields != null || excludeFields != null) req.setFetchSource(includeFields, excludeFields)

    var res = req.get()

    var hits = res.getHits.getHits
    val scrollId = res.getScrollId
    req = null
    res = null

    var currTotalFetch: Int = 0
    // check limit
    if (limitTotalFetch > 0 && (currTotalFetch + hits.length) > limitTotalFetch) {
      val newSize = limitTotalFetch - currTotalFetch
      if (newSize > 0) hits = hits.slice(0, newSize) else hits = Array.empty
    }

    if (hits.nonEmpty) for (hit <- hits) f(hit)

    currTotalFetch += hits.length
    hits = null
    if (limitTotalFetch == 0 || currTotalFetch < limitTotalFetch) {
      _scroll(scrollId, scrollKeepAliveInMs, currTotalFetch, limitTotalFetch)(f)
    } else if (scrollId != null) deleteScrollId(scrollId)
  }

  def fetchAll2(
      types: String,
      queryBuilders: QueryBuilder = null,
      sortBuilders: Seq[SortBuilder] = Nil,
      size: Int = 50,
      scrollKeepAliveInMs: Long = 60000L,
      fetchSource: Boolean = true,
      includeFields: Array[String] = null,
      excludeFields: Array[String] = null,
      limitTotalFetch: Int = 0
  )(f: (SearchHit, Int, Long) => Unit): Unit = {

    var req = client
      .prepareSearch(indexName)
      .setTypes(types)
      .setSize(size)
      .setScroll(new TimeValue(scrollKeepAliveInMs))

    if (queryBuilders != null) req.setQuery(queryBuilders)

    sortBuilders.foreach(sortBuilder => req.addSort(sortBuilder))

    if (!fetchSource) req.setFetchSource(fetchSource)
    else if (includeFields != null || excludeFields != null) req.setFetchSource(includeFields, excludeFields)

    var res = req.get()

    val totalHit = res.getHits.getTotalHits
    var hits = res.getHits.getHits
    val scrollId = res.getScrollId
    req = null
    res = null

    var currTotalFetch: Int = 0
    // check limit
    if (limitTotalFetch > 0 && (currTotalFetch + hits.length) > limitTotalFetch) {
      val newSize = limitTotalFetch - currTotalFetch
      if (newSize > 0) hits = hits.slice(0, newSize) else hits = Array.empty
    }

    if (hits.nonEmpty) for (hit <- hits) f(hit, hits.length, totalHit)

    currTotalFetch += hits.length
    hits = null

    if (limitTotalFetch == 0 || currTotalFetch < limitTotalFetch) {
      _scroll2(scrollId, scrollKeepAliveInMs, currTotalFetch, limitTotalFetch)(f)
    } else if (scrollId != null) deleteScrollId(scrollId)
  }

  def fetchAll3(
      types: String,
      queryBuilders: QueryBuilder = null,
      sortBuilders: Seq[SortBuilder] = Nil,
      size: Int = 50,
      scrollKeepAliveInMs: Long = 60000L,
      fetchSource: Boolean = true,
      includeFields: Array[String] = null,
      excludeFields: Array[String] = null,
      limitTotalFetch: Int = 0
  )(f: (Seq[SearchHit], Long) => Unit): Unit = {

    var req = client
      .prepareSearch(indexName)
      .setTypes(types)
      .setSize(size)
      .setScroll(new TimeValue(scrollKeepAliveInMs))

    if (queryBuilders != null) req.setQuery(queryBuilders)

    sortBuilders.foreach(sortBuilder => req.addSort(sortBuilder))

    if (!fetchSource) req.setFetchSource(fetchSource)
    else if (includeFields != null || excludeFields != null) req.setFetchSource(includeFields, excludeFields)

    var res =
      req.get()

    val totalHit = res.getHits.getTotalHits
    var hits = res.getHits.getHits
    val scrollId = res.getScrollId
    req = null
    res = null

    var currTotalFetch: Int = 0
    // check limit
    if (limitTotalFetch > 0 && (currTotalFetch + hits.length) > limitTotalFetch) {
      val newSize = limitTotalFetch - currTotalFetch
      if (newSize > 0) hits = hits.slice(0, newSize) else hits = Array.empty
    }

    if (hits.nonEmpty) f(hits, totalHit)

    currTotalFetch += hits.length
    hits = null

    if (limitTotalFetch == 0 || currTotalFetch < limitTotalFetch) {
      _scroll3(scrollId, scrollKeepAliveInMs, currTotalFetch, limitTotalFetch)(f)
    } else if (scrollId != null) deleteScrollId(scrollId)
  }

  def fetchAll4(
      types: Seq[String],
      customIndexNames: Seq[String] = Nil,
      queryBuilders: QueryBuilder = null,
      sortBuilders: Seq[SortBuilder] = Nil,
      size: Int = 50,
      scrollKeepAliveInMs: Long = 60000L,
      fetchSource: Boolean = true,
      includeFields: Array[String] = null,
      excludeFields: Array[String] = null,
      limitTotalFetch: Int = 0
  )(f: (Seq[SearchHit], Long) => Unit): Unit = {

    var req =
      if (customIndexNames.isEmpty) client.prepareSearch(indexName) else client.prepareSearch(customIndexNames: _*)
    req
      .setTypes(types: _*)
      .setSize(size)
      .setScroll(new TimeValue(scrollKeepAliveInMs))

    if (queryBuilders != null) req.setQuery(queryBuilders)

    sortBuilders.foreach(sortBuilder => req.addSort(sortBuilder))

    if (!fetchSource) req.setFetchSource(fetchSource)
    else if (includeFields != null || excludeFields != null) req.setFetchSource(includeFields, excludeFields)

    var res =
      req.get()

    val totalHit = res.getHits.getTotalHits
    var hits = res.getHits.getHits
    val scrollId = res.getScrollId
    req = null
    res = null

    var currTotalFetch: Int = 0
    // check limit
    if (limitTotalFetch > 0 && (currTotalFetch + hits.length) > limitTotalFetch) {
      val newSize = limitTotalFetch - currTotalFetch
      if (newSize > 0) hits = hits.slice(0, newSize) else hits = Array.empty
    }

    if (hits.nonEmpty) f(hits, totalHit)

    currTotalFetch += hits.length
    hits = null

    if (limitTotalFetch == 0 || currTotalFetch < limitTotalFetch) {
      _scroll3(scrollId, scrollKeepAliveInMs, currTotalFetch, limitTotalFetch)(f)
    } else if (scrollId != null) deleteScrollId(scrollId)
  }

  @tailrec
  private def _scroll(
      id: String,
      scrollKeepAliveInMs: Long,
      currTotalFetch: Int,
      limitTotalFetch: Int
  )(f: SearchHit => Unit): Unit = {
    var res = executeSearchScroll(id, scrollKeepAliveInMs, 0)

    val scrollId = res.getScrollId
    var hits = res.getHits.getHits
    res = null

    // check limit
    if (limitTotalFetch > 0 && (currTotalFetch + hits.length) > limitTotalFetch) {
      val newSize = limitTotalFetch - currTotalFetch
      if (newSize > 0) hits = hits.slice(0, newSize) else hits = Array.empty
    }

    hits.foreach(f)

    if (hits.nonEmpty) {
      val _currTotalFetch = currTotalFetch + hits.length
      hits = null
      if (limitTotalFetch == 0 || currTotalFetch < limitTotalFetch) {
        _scroll(scrollId, scrollKeepAliveInMs, _currTotalFetch, limitTotalFetch)(f)
      } else if (scrollId != null) deleteScrollId(scrollId)
    } else if (scrollId != null) deleteScrollId(scrollId)
  }

  @tailrec
  private def _scroll2(
      id: String,
      scrollKeepAliveInMs: Long,
      currTotalFetch: Int,
      limitTotalFetch: Int
  )(f: (SearchHit, Int, Long) => Unit): Unit = {
    var res = executeSearchScroll(id, scrollKeepAliveInMs, 0)

    val scrollId = res.getScrollId
    var hits = res.getHits.getHits
    val totalHit = res.getHits.getTotalHits
    res = null
    // check limit
    if (limitTotalFetch > 0 && (currTotalFetch + hits.length) > limitTotalFetch) {
      val newSize = limitTotalFetch - currTotalFetch
      if (newSize > 0) hits = hits.slice(0, newSize) else hits = Array.empty
    }

    if (hits.nonEmpty) for (hit <- hits) f(hit, hits.length, totalHit)

    if (hits.nonEmpty) {
      val _currTotalFetch = currTotalFetch + hits.length
      hits = null
      if (limitTotalFetch == 0 || currTotalFetch < limitTotalFetch) {
        _scroll2(scrollId, scrollKeepAliveInMs, _currTotalFetch, limitTotalFetch)(f)
      } else if (scrollId != null) deleteScrollId(scrollId)
    } else if (scrollId != null) deleteScrollId(scrollId)
  }

  @tailrec
  private def _scroll3(
      id: String,
      scrollKeepAliveInMs: Long,
      currTotalFetch: Int,
      limitTotalFetch: Int
  )(f: (Seq[SearchHit], Long) => Unit): Unit = {
    var res =
      executeSearchScroll(id, scrollKeepAliveInMs, 0)

    val scrollId = res.getScrollId
    var hits = res.getHits.getHits
    val totalHit = res.getHits.getTotalHits
    res = null

    // check limit
    if (limitTotalFetch > 0 && (currTotalFetch + hits.length) > limitTotalFetch) {
      val newSize = limitTotalFetch - currTotalFetch
      if (newSize > 0) hits = hits.slice(0, newSize) else hits = Array.empty
    }

    if (hits.nonEmpty) f(hits, totalHit)

    if (hits.nonEmpty) {
      val _currTotalFetch = currTotalFetch + hits.length
      hits = null
      if (limitTotalFetch == 0 || currTotalFetch < limitTotalFetch) {
        _scroll3(scrollId, scrollKeepAliveInMs, _currTotalFetch, limitTotalFetch)(f)
      } else if (scrollId != null) deleteScrollId(scrollId)
    } else if (scrollId != null) deleteScrollId(scrollId)
  }

  @tailrec
  private def executeSearchScroll(
      id: String,
      scrollKeepAliveInMs: Long,
      retry: Int
  ): SearchResponse = {
    try {
      val res =
        client.prepareSearchScroll(id).setScroll(new TimeValue(scrollKeepAliveInMs)).execute.actionGet

      if (res.getScrollId != id) Try(deleteScrollId(id)).toOption
      res
    } catch {
      case e: Exception =>
        if (retry > MAX_RETRY_SCROLL) throw e
        else {
          Thread.sleep(100L)
          executeSearchScroll(id, scrollKeepAliveInMs, retry + 1)
        }
    }
  }

  def deleteScrollId(scrollId: String*) = {
    client.prepareClearScroll().setScrollIds(scrollId.asJava).execute().actionGet()
  }

  // CREATE/UPDATE/DELETE

  def indexSync(
      types: String,
      id: String,
      source: String,
      refresh: Boolean = false,
      onlyNotExist: Boolean = false
  ): IndexResponse = {
    client
      .prepareIndex(indexName, types, id)
      .setSource(source)
      .setRefresh(refresh)
      .setOpType(if (onlyNotExist) IndexRequest.OpType.CREATE else IndexRequest.OpType.INDEX)
      .execute()
      .actionGet()
  }

  //id: String, source: String
  def mIndexSync(
      types: String,
      docs: Seq[(String, String)],
      refresh: Boolean = false,
      onlyNotExist: Boolean = false
  ): BulkResponse = {
    val req = client.prepareBulk()
    docs.foreach(doc => {
      val indexReq = client
        .prepareIndex(indexName, types, doc._1)
        .setSource(doc._2)
        .setRefresh(refresh)
        .setOpType(if (onlyNotExist) IndexRequest.OpType.CREATE else IndexRequest.OpType.INDEX)
      req.add(indexReq)
    })
    req.execute().actionGet()
  }

  def mIndex2Sync(
      docs: Seq[(String, String, String)],
      refresh: Boolean = false,
      onlyNotExist: Boolean = false
  ): BulkResponse = {
    val req = client.prepareBulk()
    docs.foreach(doc => {
      val indexReq = client
        .prepareIndex(indexName, doc._1, doc._2)
        .setSource(doc._3)
        .setRefresh(refresh)
        .setOpType(if (onlyNotExist) IndexRequest.OpType.CREATE else IndexRequest.OpType.INDEX)
      req.add(indexReq)
    })
    req.execute().actionGet()
  }

  def mIndex3Sync(
      index: String,
      docs: Seq[(String, String, String)],
      refresh: Boolean = false,
      onlyNotExist: Boolean = false
  ): BulkResponse = {
    val req = client.prepareBulk()
    docs.foreach(doc => {
      val indexReq = client
        .prepareIndex(index, doc._1, doc._2)
        .setSource(doc._3)
        .setRefresh(refresh)
        .setOpType(if (onlyNotExist) IndexRequest.OpType.CREATE else IndexRequest.OpType.INDEX)
      req.add(indexReq)
    })
    req.execute().actionGet()
  }

  def updateSync(
      types: String,
      id: String,
      source: String,
      allowCreate: Boolean = true,
      refresh: Boolean = false
  ): UpdateResponse = {
    client
      .prepareUpdate(indexName, types, id)
      .setDoc(source)
      .setDocAsUpsert(allowCreate)
      .setRefresh(refresh)
      .execute()
      .actionGet()
  }

  def mUpdateSync(
      types: String,
      docs: Seq[(String, String)],
      allowCreate: Boolean = true,
      refresh: Boolean = false
  ): BulkResponse = {
    val req = client.prepareBulk()
    docs.foreach(doc => {
      val updateReq = client
        .prepareUpdate(indexName, types, doc._1)
        .setDoc(doc._2)
        .setDocAsUpsert(allowCreate)
        .setRefresh(refresh)
      req.add(updateReq)
    })
    req.execute().actionGet()
  }

  /**
    * Sync action. Bulk update documents with it's version to prevent conflict
    *
    * @param types type of doc
    * @param docs  Triple value (id, doc, version)
    * @return
    */
  def mUpdateSyncConflict(types: String, docs: Seq[(String, String, Long)]): BulkResponse = {
    val req = client.prepareBulk()
    docs.foreach(doc => {
      req.add(
        client
          .prepareUpdate(indexName, types, doc._1)
          .setDoc(doc._2)
          .setVersion(doc._3)
          .setVersionType(VersionType.INTERNAL)
          .setDocAsUpsert(true)
      )
    })
    req.get()
  }

  def deleteSync(types: String, id: String, refresh: Boolean = false): DeleteResponse = {
    client
      .prepareDelete(indexName, types, id)
      .setRefresh(refresh)
      .execute()
      .actionGet()
  }

  def mDeleteSync(types: String, ids: Seq[String], refresh: Boolean = false): BulkResponse = {
    val bulkReq = client.prepareBulk()
    ids.foreach(id => bulkReq.add(new DeleteRequest(indexName, types, id).refresh(refresh)))
    bulkReq.execute().actionGet()
  }
}

case class ElasticSearchClient(client: TransportClient, indexName: String) extends BaseElasticSearchClient {

  def usingWithOtherIndex(otherIndexName: String): ElasticSearchClient = {
    new ElasticSearchClient(client, otherIndexName)
  }

}

object ElasticSearchClient {
  private val clientMap = scala.collection.mutable.Map.empty[String, ElasticSearchClient]

  /**
    *
    * @param config
    *
    * - RV_ES_SERVERS
    *
    * - RV_ES_CLUSTER_NAME
    *
    * - RV_ES_INDEX_NAME
    *
    * - RV_ES_TRAFFIC_SNIFF
    * @return
    */
  def client(config: Config): ElasticSearchClient = {
    val servers = config.getList("RV_ES_SERVERS", ",").asScala.toSeq
    val clusterName = config.get("RV_ES_CLUSTER_NAME")
    val indexName = config.get("RV_ES_INDEX_NAME")
    val transportSniff = config.getBoolean("RV_ES_TRAFFIC_SNIFF", false)

    ElasticSearchClient.build(servers, clusterName, indexName, transportSniff)

  }

  def build(
      servers: Seq[String],
      clusterName: String,
      indexName: String,
      transportSniff: Boolean = false
  ): ElasticSearchClient = {

    val cacheKey = s"${servers.sorted.mkString(",")}_${clusterName}"

    clientMap.get(cacheKey) match {
      case Some(client) => client
      case None =>
        clientMap.synchronized {
          clientMap
            .get(cacheKey)
            .fold[ElasticSearchClient]({
              val settings = Settings
                .builder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", transportSniff)
                .build()

              val rawCli: TransportClient = TransportClient.builder().settings(settings).build()
              servers map (x => x.split(':')) filter (_.length == 2) foreach (hostAndPort => {
                rawCli.addTransportAddress(
                  new InetSocketTransportAddress(InetAddress.getByName(hostAndPort(0)), hostAndPort(1).toInt)
                )
              })
              val client = new ElasticSearchClient(rawCli, indexName)
              clientMap.put(cacheKey, client)
              client
            })(x => x)

        }
    }

  }
}
