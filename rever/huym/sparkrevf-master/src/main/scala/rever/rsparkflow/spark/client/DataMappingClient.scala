package rever.rsparkflow.spark.client

import com.fasterxml.jackson.databind.JsonNode
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.domain.contact.{CoreContact, PContact}
import rever.rsparkflow.spark.domain.sale_pipeline.{SalePipeline, SalePipelinePhase}
import rever.rsparkflow.spark.domain.user.{Team, User}
import rever.rsparkflow.spark.utils.JsonUtils
import scalaj.http.{Http, HttpResponse}

/**
  * @author anhlt (andy)
  * */
object DataMappingClient {
  final val DEFAULT_CON_TIMEOUT_MS = 15000
  final val DEFAULT_READ_TIMEOUT_MS = 60000

  private val clientMap = scala.collection.mutable.Map.empty[String, DataMappingClient]

  /**
    *
    * @param config
    *
    * - RV_DATA_MAPPING_HOST
    *
    * - RV_DATA_MAPPING_USER
    *
    * - RV_DATA_MAPPING_PASSWORD
    *
    * - RV_DATA_MAPPING_CONNECT_TIMEOUT_MS
    *
    * - RV_DATA_MAPPING_READ_TIMEOUT_MS
    * @return
    */
  def client(config: Config): DataMappingClient = {
    val url = config.get("RV_DATA_MAPPING_HOST")
    val user = config.get("RV_DATA_MAPPING_USER")
    val password = config.get("RV_DATA_MAPPING_PASSWORD")

    val connTimeoutMs = config.getInt("RV_DATA_MAPPING_CONNECT_TIMEOUT_MS", 15000)
    val readTimeoutMs = config.getInt("RV_DATA_MAPPING_READ_TIMEOUT_MS", 60000)
    client(url, user, password, Some(connTimeoutMs), Some(readTimeoutMs))
  }

  def client(
      url: String,
      user: String,
      password: String,
      connTimeoutMs: Option[Int] = Some(15000),
      readTimeoutMs: Option[Int] = Some(60000)
  ): DataMappingClient = {
    val cacheKey = s"$url:$user:$password"
    clientMap.get(cacheKey) match {
      case Some(client) => client
      case None =>
        clientMap.synchronized {
          clientMap
            .get(cacheKey)
            .fold[DataMappingClient]({
              val client = DataMappingClient(url, user, password, connTimeoutMs, readTimeoutMs)
              clientMap.put(cacheKey, client)
              client
            })(x => x)

        }
    }
  }
}

case class DataMappingClient(
    host: String,
    user: String,
    password: String,
    connTimeoutMs: Option[Int] = Some(15000),
    readTimeoutMs: Option[Int] = Some(60000)
) {

  def mGetInquiryPropertyIds(inquiryIds: Seq[String]): Map[String, Seq[String]] = {

    val request = Http(s"$host/inquiry/matching/property_ids")
      .timeout(
        connTimeoutMs.getOrElse(DataMappingClient.DEFAULT_CON_TIMEOUT_MS),
        readTimeoutMs.getOrElse(DataMappingClient.DEFAULT_READ_TIMEOUT_MS)
      )
      .header("content-type", "application/json")
      .auth(user, password)
      .postData(
        JsonUtils.toJson(Map("inquiry_ids" -> inquiryIds))
      )

    val response = request.asString

    toEntity[Map[String, Seq[String]]](response)

  }

  def mGetPContact(pCids: Seq[String]): Map[String, PContact] = {

    val request = Http(s"$host/contact/personal/mget")
      .timeout(
        connTimeoutMs.getOrElse(DataMappingClient.DEFAULT_CON_TIMEOUT_MS),
        readTimeoutMs.getOrElse(DataMappingClient.DEFAULT_READ_TIMEOUT_MS)
      )
      .header("content-type", "application/json")
      .auth(user, password)
      .postData(
        JsonUtils.toJson(Map("p_cids" -> pCids))
      )

    val response = request.asString

    toEntity[Map[String, PContact]](response)

  }

  def mGetSysContact(cids: Seq[String]): Map[String, CoreContact] = {

    val request = Http(s"$host/contact/sys/mget")
      .timeout(
        connTimeoutMs.getOrElse(DataMappingClient.DEFAULT_CON_TIMEOUT_MS),
        readTimeoutMs.getOrElse(DataMappingClient.DEFAULT_READ_TIMEOUT_MS)
      )
      .header("content-type", "application/json")
      .auth(user, password)
      .postData(
        JsonUtils.toJson(Map("cids" -> cids))
      )

    val response = request.asString

    toEntity[Map[String, CoreContact]](response)

  }

  def mGetFrontendUserProfiles(usernames: Seq[String]): Map[String, JsonNode] = {

    val request = Http(s"$host/frontend-userprofile/mget")
      .timeout(connTimeoutMs.getOrElse(15000), readTimeoutMs.getOrElse(60000))
      .header("content-type", "application/json")
      .auth(user, password)
      .postData(
        JsonUtils.toJson(Map("usernames" -> usernames))
      )

    val response = request.asString

    toEntity[Map[String, JsonNode]](response)

  }

  def mGetSalePipelines(): Map[Long, SalePipeline] = {

    val request = Http(s"$host/sale_pipeline/all")
      .timeout(
        connTimeoutMs.getOrElse(DataMappingClient.DEFAULT_CON_TIMEOUT_MS),
        readTimeoutMs.getOrElse(DataMappingClient.DEFAULT_READ_TIMEOUT_MS)
      )
      .header("content-type", "application/json")
      .auth(user, password)
      .postData(
        JsonUtils.toJson(Map())
      )

    val response = request.asString

    toEntity[Map[Long, JsonNode]](response)
      .map {
        case (pipelineId, node) => pipelineId -> SalePipeline(node)
      }

  }

  def mGetSalePipelines(pipelineIds: Seq[Long]): Map[Long, SalePipeline] = {

    val request = Http(s"$host/sale_pipeline/mget")
      .timeout(
        connTimeoutMs.getOrElse(DataMappingClient.DEFAULT_CON_TIMEOUT_MS),
        readTimeoutMs.getOrElse(DataMappingClient.DEFAULT_READ_TIMEOUT_MS)
      )
      .header("content-type", "application/json")
      .auth(user, password)
      .postData(
        JsonUtils.toJson(Map("pipeline_ids" -> pipelineIds))
      )

    val response = request.asString

    toEntity[Map[Long, JsonNode]](response)
      .map {
        case (pipelineId, node) => pipelineId -> SalePipeline(node)
      }

  }

  def mGetSalePipelinePhases(phaseIds: Seq[Long]): Map[Long, SalePipelinePhase] = {

    val request = Http(s"$host/sale_pipeline/phases/mget")
      .timeout(
        connTimeoutMs.getOrElse(DataMappingClient.DEFAULT_CON_TIMEOUT_MS),
        readTimeoutMs.getOrElse(DataMappingClient.DEFAULT_READ_TIMEOUT_MS)
      )
      .header("content-type", "application/json")
      .auth(user, password)
      .postData(
        JsonUtils.toJson(Map("phase_ids" -> phaseIds))
      )

    val response = request.asString

    toEntity[Map[Long, SalePipelinePhase]](response)

  }

  def getAllSortedSalePipelineStages(pipelineIds: Seq[Long]): Map[Long, Seq[Long]] = {

    val request = Http(s"$host/sale_pipeline/stage_ids/list")
      .timeout(
        connTimeoutMs.getOrElse(DataMappingClient.DEFAULT_CON_TIMEOUT_MS),
        readTimeoutMs.getOrElse(DataMappingClient.DEFAULT_READ_TIMEOUT_MS)
      )
      .header("content-type", "application/json")
      .auth(user, password)
      .postData(
        JsonUtils.toJson(Map("pipeline_ids" -> pipelineIds))
      )

    val response = request.asString

    toEntity[Map[Long, Seq[Long]]](response)

  }

  def mGetIdFromAlias(aliases: Seq[String]): Map[String, String] = {
    val request = Http(s"$host/id-mapping/mget")
      .timeout(
        connTimeoutMs.getOrElse(DataMappingClient.DEFAULT_CON_TIMEOUT_MS),
        readTimeoutMs.getOrElse(DataMappingClient.DEFAULT_READ_TIMEOUT_MS)
      )
      .header("content-type", "application/json")
      .auth(user, password)
      .postData(
        JsonUtils.toJson(Map("aliases" -> aliases))
      )

    val response = request.asString

    toEntity[Map[String, String]](response)
  }

  def mGetUser(usernames: Seq[String]): Map[String, User] = {

    val request = Http(s"$host/staff/username/mget")
      .timeout(
        connTimeoutMs.getOrElse(DataMappingClient.DEFAULT_CON_TIMEOUT_MS),
        readTimeoutMs.getOrElse(DataMappingClient.DEFAULT_READ_TIMEOUT_MS)
      )
      .header("content-type", "application/json")
      .auth(user, password)
      .postData(
        JsonUtils.toJson(Map("usernames" -> usernames))
      )

    val response = request.asString

    toEntity[Map[String, JsonNode]](response).map(entry => entry._1 -> User(entry._2))

  }

  def mGetUserByName(names: Seq[String]): Map[String, User] = {
    val request = Http(s"$host/staff/name/mget")
      .timeout(
        connTimeoutMs.getOrElse(DataMappingClient.DEFAULT_CON_TIMEOUT_MS),
        readTimeoutMs.getOrElse(DataMappingClient.DEFAULT_READ_TIMEOUT_MS)
      )
      .header("content-type", "application/json")
      .auth(user, password)
      .postData(
        JsonUtils.toJson(Map("names" -> names))
      )

    val response = request.asString

    toEntity[Map[String, JsonNode]](response).map(entry => entry._1 -> User(entry._2))
  }

  def mGetUserByEmployeeId(names: Seq[String]): Map[String, User] = {
    val request = Http(s"$host/staff/employee_id/mget")
      .timeout(
        connTimeoutMs.getOrElse(DataMappingClient.DEFAULT_CON_TIMEOUT_MS),
        readTimeoutMs.getOrElse(DataMappingClient.DEFAULT_READ_TIMEOUT_MS)
      )
      .header("content-type", "application/json")
      .auth(user, password)
      .postData(
        JsonUtils.toJson(Map("employee_ids" -> names))
      )

    val response = request.asString

    toEntity[Map[String, JsonNode]](response).map(entry => entry._1 -> User(entry._2))
  }

  def mGetUserByEmail(emails: Seq[String]): Map[String, User] = {

    val request = Http(s"$host/staff/email/mget")
      .timeout(
        connTimeoutMs.getOrElse(DataMappingClient.DEFAULT_CON_TIMEOUT_MS),
        readTimeoutMs.getOrElse(DataMappingClient.DEFAULT_READ_TIMEOUT_MS)
      )
      .header("content-type", "application/json")
      .auth(user, password)
      .postData(
        JsonUtils.toJson(Map("emails" -> emails))
      )

    val response = request.asString

    toEntity[Map[String, JsonNode]](response).map(entry => entry._1 -> User(entry._2))

  }

  def mGetTeam(teamIds: Seq[Int]): Map[Int, Team] = {
    val request = Http(s"$host/team/ids/mget")
      .timeout(
        connTimeoutMs.getOrElse(DataMappingClient.DEFAULT_CON_TIMEOUT_MS),
        readTimeoutMs.getOrElse(DataMappingClient.DEFAULT_READ_TIMEOUT_MS)
      )
      .header("content-type", "application/json")
      .auth(user, password)
      .postData(
        JsonUtils.toJson(Map("team_ids" -> teamIds))
      )

    val response = request.asString

    toEntity[Map[Int, JsonNode]](response).map(entry => entry._1 -> Team(entry._2))
  }

  def mGetTeamByName(names: Seq[String]): Map[String, Team] = {
    val request = Http(s"$host/team/name/mget")
      .timeout(
        connTimeoutMs.getOrElse(DataMappingClient.DEFAULT_CON_TIMEOUT_MS),
        readTimeoutMs.getOrElse(DataMappingClient.DEFAULT_READ_TIMEOUT_MS)
      )
      .header("content-type", "application/json")
      .auth(user, password)
      .postData(
        JsonUtils.toJson(Map("names" -> names))
      )

    val response = request.asString

    toEntity[Map[String, JsonNode]](response).map(entry => entry._1 -> Team(entry._2))
  }

  def mGetTeamByAlias(alias: Seq[String]): Map[String, Team] = {
    val request = Http(s"$host/team/alias/mget")
      .timeout(
        connTimeoutMs.getOrElse(DataMappingClient.DEFAULT_CON_TIMEOUT_MS),
        readTimeoutMs.getOrElse(DataMappingClient.DEFAULT_READ_TIMEOUT_MS)
      )
      .header("content-type", "application/json")
      .auth(user, password)
      .postData(
        JsonUtils.toJson(Map("alias" -> alias))
      )

    val response = request.asString

    toEntity[Map[String, JsonNode]](response).map(entry => entry._1 -> Team(entry._2))

  }

  def mGetJobTitleByAlias(alias: Seq[String]): Map[String, String] = {
    val request = Http(s"$host/property_option/job_title/data")
      .timeout(
        connTimeoutMs.getOrElse(DataMappingClient.DEFAULT_CON_TIMEOUT_MS),
        readTimeoutMs.getOrElse(DataMappingClient.DEFAULT_READ_TIMEOUT_MS)
      )
      .header("content-type", "application/json")
      .auth(user, password)

    val response = request.asString

    val jobTitleMap = toEntity[Seq[JsonNode]](response)
      .map(node => {
        val alias = node.at("/internal_value").asText("")
        val name = node.at("/name").asText("")

        alias -> name
      })
      .toMap
      .filterNot(_._1.isEmpty)

    alias
      .filter(jobTitleMap.contains)
      .map(alias => alias -> jobTitleMap.getOrElse(alias, ""))
      .toMap
  }

  private def toEntity[T: Manifest](response: HttpResponse[String]): T = {
    if (!response.is2xx) {
      throw new Exception(s"Failed to execute: ${response.statusLine} - ${response.body}")
    }
    JsonUtils.fromJson[T](response.body)

  }
}
