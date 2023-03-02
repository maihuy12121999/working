package rever.rsparkflow.spark.client

import com.fasterxml.jackson.databind.JsonNode
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.domain.data_delivery.DataDeliveryMsg
import rever.rsparkflow.spark.utils.JsonUtils
import scalaj.http.Http

import scala.concurrent.duration.DurationInt

case class PushKafkaRecord(key: Option[String], value: String)

object PushKafkaClient {
  private val clientMap = scala.collection.mutable.Map.empty[String, PushKafkaClient]

  def client(config: Config): PushKafkaClient = {
    val url = config.get("RV_PUSH_KAFKA_HOST")
    val sk = config.get("RV_PUSH_KAFKA_SK")
    client(url, sk)
  }

  def client(url: String, sk: String): PushKafkaClient = {
    val cacheKey = s"$url:$sk"

    clientMap.get(cacheKey) match {
      case Some(client) => client
      case None =>
        clientMap.synchronized {
          clientMap
            .get(cacheKey)
            .fold[PushKafkaClient]({
              val client = PushKafkaClient(url, sk)
              clientMap.put(cacheKey, client)
              client
            })(x => x)

        }
    }

  }
}

case class PushKafkaClient(host: String, sk: String) {

  def multiPushKafka(topic: String, records: Seq[PushKafkaRecord]): Long = {
    val response = multiPush(topic, JsonUtils.toJson(Map("records" -> records)))
    val result = JsonUtils.fromJson[Seq[JsonNode]](response)
    result.size
  }

  def multiDeliverDataToKafka(topic: String, records: Seq[DataDeliveryMsg]): Long = {
    val pushKafkaRecords = records.map(msg => PushKafkaRecord(key = Some("1"), value = JsonUtils.toJson(msg, false)))
    val response = multiPush(topic, JsonUtils.toJson(Map("records" -> pushKafkaRecords)))
    val result = JsonUtils.fromJson[Seq[JsonNode]](response)
    result.size
  }

  private def multiPush(topic: String, body: String): String = {
    val request = Http(s"$host/push_kafka/${topic}/multi")
      .timeout(15.seconds.toMillis.toInt, 30.seconds.toMillis.toInt)
      .header("content-type", "application/json")
      .param("sk", sk)
      .postData(body)

    val response = request.asString

    if (!response.is2xx) {
      throw new Exception(s"Failed to push to kafka: ${response.body}")
    }

    response.body
  }

}
