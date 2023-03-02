package rever.rsparkflow.spark.client

import com.fasterxml.jackson.databind.JsonNode
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.utils.JsonUtils
import scalaj.http.Http

/**
  * @author anhlt (andy)
  * @since 23/04/2022
**/

object RapIngestionClient {
  private val clientMap = scala.collection.mutable.Map.empty[String, RapIngestionClient]

  def client(config: Config): RapIngestionClient = {
    val url = config.get("RV_RAP_INGESTION_HOST")
    client(url)
  }

  def client(url: String): RapIngestionClient = {
    clientMap.get(url) match {
      case Some(client) => client
      case None =>
        clientMap.synchronized {
          clientMap
            .get(url)
            .fold[RapIngestionClient]({
              val client = RapIngestionClient(url)
              clientMap.put(url, client)
              client
            })(x => x)

        }
    }
  }
}

case class RapIngestionClient(host: String) {

  def ingest(topic: String, records: Seq[JsonNode]): Int = {
    val request = Http(s"$host/ingestion")
      .timeout(15000, 60000)
      .header("content-type", "application/json")
      .postData(
        JsonUtils.toJson(
          Map(
            "rap_topic" -> topic,
            "version" -> "1",
            "records" -> records
          )
        )
      )

    val response = request.asString

    if (!response.is2xx) {
      throw new Exception(s"Failed to ingest data: ${response.body}")
    }

    val result = JsonUtils.fromJson[JsonNode](response.body)

    result.at("/data").asInt(0)
  }

  def forceMerge(topic: String, isForceRewritePart: Boolean = false): Boolean = {
    try {
      val request = Http(s"$host/ingestion/force_merge")
        .timeout(15000, 60000)
        .header("content-type", "application/json")
        .postData(
          JsonUtils.toJson(
            Map(
              "rap_topic" -> topic,
              "is_force_rewrite_part" -> isForceRewritePart,
              "version" -> "1"
            )
          )
        )

      val response = request.asString

      val result = JsonUtils.fromJson[JsonNode](response.body)

      result.at("/data").asBoolean(false)
    } catch {
      case _ => false
    }
  }

}
