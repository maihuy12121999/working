package rever.etl.support.client

import rever.rsparkflow.spark.utils.JsonUtils
import rever.etl.support.domain.CreateListingFeatureVectorRecord
import scalaj.http.Http
case class ListingMatchingVectorConfig(listingMatchingUrl: String, batchSize: Option[Int])

object ListingMatchingClient {
  private val clientMap = scala.collection.mutable.Map.empty[String, ListingMatchingClient]

  def client(config: ListingMatchingVectorConfig): ListingMatchingClient = {

    val cacheKey = config.listingMatchingUrl
    clientMap.get(cacheKey) match {
      case Some(client) => client
      case None =>
        clientMap.synchronized {
          clientMap
            .get(cacheKey)
            .fold[ListingMatchingClient]({
              val client = ListingMatchingClient(config.listingMatchingUrl, config.batchSize)
              clientMap.put(cacheKey, client)
              client
            })(x => x)

        }
    }
  }
}

case class ListingMatchingClient(listingMatchingUrl: String, batchSize: Option[Int]) extends Serializable {

  def insertListingVectors(listings: Seq[CreateListingFeatureVectorRecord]): Int = {

    listings
      .grouped(batchSize.getOrElse(200))
      .map(listings => {
        val body = JsonUtils.toJson(listings, false)
        println(body)
        val response = Http(s"${listingMatchingUrl}/listing/update")
          .put(body)
          .header("content-type", "application/json")
          .asString

        val responseNode = JsonUtils.toJsonNode(response.body)
        if (response.code == 200 && responseNode.at("/Success").asBoolean(false)) {
          listings.length
        } else {
          0
        }
      })
      .sum

  }
}
