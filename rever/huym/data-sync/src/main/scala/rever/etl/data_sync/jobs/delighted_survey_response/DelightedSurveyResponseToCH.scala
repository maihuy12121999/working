package rever.etl.data_sync.jobs.delighted_survey_response

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, collect_list, element_at, from_json, lit}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import rever.etl.data_sync.jobs.delighted_survey_response.DelightedSurveyResponseHelper._
import rever.etl.data_sync.jobs.delighted_survey_response.reader.DelightedSurveyResponseReader
import rever.etl.data_sync.jobs.delighted_survey_response.writer.DelightedSurveyResponseWriter
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.client.DataMappingClient
import rever.etl.rsparkflow.utils.{JsonUtils, TimestampUtils}

import java.text.SimpleDateFormat
import scala.collection.mutable
import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter

class DelightedSurveyResponseToCH extends FlowMixin {
  @Output(writer = classOf[DelightedSurveyResponseWriter])
  @Table("delighted_survey_response")
  def build(
      @Table(
        name = "delighted_survey",
        reader = classOf[DelightedSurveyResponseReader]
      ) delightedSurveyDf: DataFrame,
      config: Config
  ): DataFrame = {
    val df = standardizedData(delightedSurveyDf)
      .withColumn(
        DelightedSurveyResponseHelper.NOTES,
        from_json(col(DelightedSurveyResponseHelper.NOTES), ArrayType(StringType)).cast(ArrayType(StringType))
      )
      .withColumn(
        DelightedSurveyResponseHelper.TAGS,
        from_json(col(DelightedSurveyResponseHelper.TAGS), ArrayType(StringType)).cast(ArrayType(StringType))
      )
      .mapPartitions(rows => rows.toSeq.grouped(500).flatMap(enhanceData(_, surveyResponseSchema, config)))(RowEncoder(surveyResponseSchema))
      .withColumn(
        DelightedSurveyResponseHelper.LOG_TIME,
        lit(System.currentTimeMillis())
      )

    df
  }

  def standardizedData(df: DataFrame): DataFrame = {
    df
      .sort(col(DelightedSurveyResponseHelper.UPDATED_TIME).asc)
      .groupBy(
        col(DelightedSurveyResponseHelper.SURVEY_ID)
      )
      .agg(
        collect_list(col(DelightedSurveyResponseHelper.SURVEY_SYSTEM)).as("list_survey_system"),
        collect_list(col(DelightedSurveyResponseHelper.PUBLIC_SURVEY_ID)).as("list_public_survey_id"),
        collect_list(col(DelightedSurveyResponseHelper.PROJECT_ID)).as("list_project_id"),
        collect_list(col(DelightedSurveyResponseHelper.CHANNEL)).as("list_channel"),
        collect_list(col(DelightedSurveyResponseHelper.SURVEY_TYPE)).as("list_survey_types"),
        collect_list(col(DelightedSurveyResponseHelper.PHASE)).as("list_phases"),
        collect_list(col(DelightedSurveyResponseHelper.BUSINESS_UNIT)).as("list_business_units"),
        collect_list(col(DelightedSurveyResponseHelper.MARKET_CENTER_ID)).as("list_market_center_id"),
        collect_list(col(DelightedSurveyResponseHelper.TEAM_ID)).as("list_team_id"),
        collect_list(col(DelightedSurveyResponseHelper.AGENT_ID)).as("list_agent_id"),
        collect_list(col(DelightedSurveyResponseHelper.CID)).as("list_cid"),
        collect_list(col(DelightedSurveyResponseHelper.P_CID)).as("list_p_cid"),
        collect_list(col(DelightedSurveyResponseHelper.SCORE)).as("list_score"),
        collect_list(col(DelightedSurveyResponseHelper.COMMENT)).as("list_comment"),
        collect_list(col(DelightedSurveyResponseHelper.PROPERTIES)).as("list_properties"),
        collect_list(col(DelightedSurveyResponseHelper.NOTES)).as("list_notes"),
        collect_list(col(DelightedSurveyResponseHelper.TAGS)).as("list_tags"),
        collect_list(col(DelightedSurveyResponseHelper.SENT_INFO)).as("list_sent_info"),
        collect_list(col("response")).as("list_response"),
        collect_list(col(DelightedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO)).as("list_last_access_client_info"),
        collect_list(col(DelightedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO_PER_CHANNEL)).as("list_last_access_client_info_per_channel"),
        collect_list(col(DelightedSurveyResponseHelper.CREATED_TIME)).as("list_created_time"),
        collect_list(col(DelightedSurveyResponseHelper.UPDATED_TIME)).as("list_updated_time"),
        collect_list(col(DelightedSurveyResponseHelper.SENT_TIME)).as("list_sent_time"),
        collect_list(col("submit_time")).as("list_submit_time")
      )
      .select(
        col(DelightedSurveyResponseHelper.SURVEY_ID),
        element_at(col("list_survey_system"),-1).as(DelightedSurveyResponseHelper.SURVEY_SYSTEM),
        element_at(col("list_public_survey_id"),-1).as(DelightedSurveyResponseHelper.PUBLIC_SURVEY_ID),
        element_at(col("list_project_id"),-1).as(DelightedSurveyResponseHelper.PROJECT_ID),
        element_at(col("list_channel"),-1).as(DelightedSurveyResponseHelper.CHANNEL),
        element_at(col("list_survey_types"),-1).as(DelightedSurveyResponseHelper.SURVEY_TYPE),
        element_at(col("list_phases"),-1).as(DelightedSurveyResponseHelper.PHASE),
        element_at(col("list_business_units"),-1).as(DelightedSurveyResponseHelper.BUSINESS_UNIT),
        element_at(col("list_market_center_id"),-1).as(DelightedSurveyResponseHelper.MARKET_CENTER_ID),
        element_at(col("list_team_id"),-1).as(DelightedSurveyResponseHelper.TEAM_ID),
        element_at(col("list_agent_id"),-1).as(DelightedSurveyResponseHelper.AGENT_ID),
        element_at(col("list_cid"),-1).as(DelightedSurveyResponseHelper.CID),
        element_at(col("list_p_cid"),-1).as(DelightedSurveyResponseHelper.P_CID),
        element_at(col("list_score"),-1).as(DelightedSurveyResponseHelper.SCORE),
        col("list_score"),
        element_at(col("list_comment"),-1).as(DelightedSurveyResponseHelper.COMMENT),
        element_at(col("list_properties"),-1).as(DelightedSurveyResponseHelper.PROPERTIES),
        element_at(col("list_notes"),-1).as(DelightedSurveyResponseHelper.NOTES),
        element_at(col("list_tags"),-1).as(DelightedSurveyResponseHelper.TAGS),
        element_at(col("list_sent_info"),-1).as(DelightedSurveyResponseHelper.SENT_INFO),
        element_at(col("list_response"),-1).as(DelightedSurveyResponseHelper.LAST_RESPONSE),
        col("list_response").as(DelightedSurveyResponseHelper.RESPONSES),
        element_at(col("list_last_access_client_info"),-1).as(DelightedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO),
        element_at(col("list_last_access_client_info_per_channel"),-1).as(DelightedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO_PER_CHANNEL),
        element_at(col("list_created_time"),-1).as(DelightedSurveyResponseHelper.CREATED_TIME),
        element_at(col("list_updated_time"),-1).as(DelightedSurveyResponseHelper.UPDATED_TIME),
        element_at(col("list_sent_time"),-1).as(DelightedSurveyResponseHelper.SENT_TIME),
        element_at(col("list_submit_time"),1).as(DelightedSurveyResponseHelper.FIRST_SUBMIT_TIME),
        element_at(col("list_submit_time"),-1).as(DelightedSurveyResponseHelper.LAST_SUBMIT_TIME)
      )

  }

  def enhanceData(rows:Seq[Row], schema: StructType, config: Config): Seq[Row] = {
    val client = DataMappingClient.client(config)
    val mapping = Mapping(
      JsonUtils.fromJson[Map[String,String]](config.getAndDecodeBase64("phase_standardized_mapping")),
      JsonUtils.fromJson[Map[String,String]](config.getAndDecodeBase64("phase_to_stage_mapping"))
    )

    val rvaEmails = rows
      .map(row => getPossibleEmail(row))
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .map(_.trim)
      .distinct

    val agentIdMapByEmail = mGetAgentIdFromEmail(rvaEmails, client)

    val agentIds = rows
      .map(row => getPossibleAgentId(row, agentIdMapByEmail))
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .distinct

    val teamNames = rows
      .map(row => {
        getPossibleTeamName(row)
      })
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .map(_.trim)
      .distinct

    val teamIdMapByName = mGetIdFromName(teamNames,client)
    val teamIdMapByAgent = mGetTeamIdFromAgent(agentIds,client)

    val teamIds = rows
      .map(row => {
        val agentId = getPossibleAgentId(row, agentIdMapByEmail)
        getPossibleTeamIds(row,teamIdMapByName,teamIdMapByAgent, agentId)
      })
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .map(_.trim)
      .distinct

    val teamNameMap = mGetNameFromId(teamIds.map(_.toInt),client)

    val marketCenterNames = rows
      .map(row =>{
        val agentId = getPossibleAgentId(row, agentIdMapByEmail)
        val teamId = getPossibleTeamIds(row,teamIdMapByName,teamIdMapByAgent, agentId)
        val teamName = Seq(teamNameMap.getOrElse(teamId,""), getPossibleTeamName(row))
            .filterNot(_.isEmpty)
            .filterNot(_ == null)
            .headOption
            .getOrElse("")
        getPossibleMcNames(row, teamName)
      })
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .map(_.trim)
      .distinct

    val mcIdMapByName = mGetIdFromName(marketCenterNames, client)
    val mcIdMapByAgent = mGetMcIdFromAgent(agentIds, client)

    val marketCenterIds = rows
      .map(row =>{
        val agentId = getPossibleAgentId(row, agentIdMapByEmail)
        val teamId = getPossibleTeamIds(row,teamIdMapByName,teamIdMapByAgent, agentId)
        val teamName = teamNameMap.getOrElse(teamId,"")
        val mcName = getPossibleMcNames(row, teamName)
        getPossibleMcId(row, mcIdMapByName, mcIdMapByAgent, agentId, mcName)
      })
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .map(_.trim)
      .distinct

    val mcNameMap = mGetNameFromId(marketCenterIds.map(_.toInt), client)

    val oppoIds = rows
      .map(row => {
        val properties = row.getAs[String](DelightedSurveyResponseHelper.PROPERTIES)
        getFieldFromJsonString("/id_co_hoi",properties).getOrElse("")
      })
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .map(_.trim)
      .distinct

    val oppoToPipelineIdMap = mGetPipelineIdFromOppo(oppoIds, client)
    rows.map(row => {
      val surveySystem = row.getAs[String](DelightedSurveyResponseHelper.SURVEY_SYSTEM)
      val surveyId = row.getAs[String](DelightedSurveyResponseHelper.SURVEY_ID).trim
      val publicSurveyId = row.getAs[String](DelightedSurveyResponseHelper.PUBLIC_SURVEY_ID).trim
      val projectId = row.getAs[String](DelightedSurveyResponseHelper.PROJECT_ID).trim
      val channel = row.getAs[String](DelightedSurveyResponseHelper.CHANNEL).trim.toLowerCase
      val surveyType = row.getAs[String](DelightedSurveyResponseHelper.SURVEY_TYPE).trim.toLowerCase
      var businessUnit = row.getAs[String](DelightedSurveyResponseHelper.BUSINESS_UNIT).trim.toLowerCase
      var cid = row.getAs[String](DelightedSurveyResponseHelper.CID).trim
      var pCid = row.getAs[String](DelightedSurveyResponseHelper.P_CID).trim
      val score = row.getAs[Int](DelightedSurveyResponseHelper.SCORE)
      val comment = row.getAs[String](DelightedSurveyResponseHelper.COMMENT).trim
      val properties = row.getAs[String](DelightedSurveyResponseHelper.PROPERTIES).trim
      val notes = row.getAs[mutable.WrappedArray[String]](DelightedSurveyResponseHelper.NOTES).toArray
      val tags = row.getAs[mutable.WrappedArray[String]](DelightedSurveyResponseHelper.TAGS).toArray
      val sentInfo = row.getAs[String](DelightedSurveyResponseHelper.SENT_INFO).trim
      val lastResponse = row.getAs[String](DelightedSurveyResponseHelper.LAST_RESPONSE).trim
      val responses = row.getAs[mutable.WrappedArray[String]](DelightedSurveyResponseHelper.RESPONSES).toArray
      val lastAccessClientInfo = row.getAs[String](DelightedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO)
      val lastAccessClientInfoPerChannel = row.getAs[String](DelightedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO_PER_CHANNEL)
      val createdTime = row.getAs[Long](DelightedSurveyResponseHelper.CREATED_TIME)
      val updatedTime = row.getAs[Long](DelightedSurveyResponseHelper.UPDATED_TIME)
      val lastSubmitTime = row.getAs[Long](DelightedSurveyResponseHelper.LAST_SUBMIT_TIME)
      val firstSubmitTime = row.getAs[Long](DelightedSurveyResponseHelper.FIRST_SUBMIT_TIME)

      val agentId = getPossibleAgentId(row, agentIdMapByEmail)

      val teamId = getPossibleTeamIds(row, teamIdMapByName, teamIdMapByAgent, agentId)

      val teamName = teamNameMap.getOrElse(teamId,"")
      val marketCenterName = getPossibleMcNames(row, teamName)
      val marketCenterId = getPossibleMcId(row, mcIdMapByName,mcIdMapByAgent, agentId, marketCenterName)
      val oppoId = getFieldFromJsonString("/id_co_hoi",properties).getOrElse("")
      val pipelineId = oppoToPipelineIdMap.getOrElse(oppoId,"")

      businessUnit = getPossibleBusinessUnit(row, businessUnit, marketCenterId, mcNameMap, teamNameMap, teamId, pipelineId, config)

      val phase = standardizedPhase(row, mapping)

      val stage = mappingStage(phase, mapping)

      cid = cid match{
        case "" => getCid(sentInfo, agentId)
        case _ => cid
      }

      pCid = pCid match{
        case "" => getPCid(sentInfo, agentId)
        case _ => pCid
      }

      val totalResponseScore = row.getAs[mutable.WrappedArray[Int]]("list_score")
        .toArray
        .sum

      val totalResponse = responses.length

      val totalAccess = responses.length

      val sentTime = getPossibleInteractionTime(row)

      new GenericRowWithSchema(
        Array(
          surveySystem,
          surveyId,
          publicSurveyId,
          projectId,
          channel,
          surveyType,
          phase,
          stage,
          pipelineId,
          businessUnit,
          marketCenterId,
          teamId,
          agentId,
          cid,
          pCid,
          score,
          comment,
          properties,
          notes,
          tags,
          sentInfo,
          lastResponse,
          responses,
          totalResponseScore,
          totalResponse,
          totalAccess,
          lastAccessClientInfo,
          lastAccessClientInfoPerChannel,
          createdTime,
          updatedTime,
          sentTime,
          firstSubmitTime,
          lastSubmitTime
        ),
        schema
      )

    })
    }
  private def getPossibleEmail(row: Row): String = {
    val properties = row.getAs[String](DelightedSurveyResponseHelper.PROPERTIES)
    Seq(
    getFieldFromJsonString("/Rva_email",properties).getOrElse(""),
    getFieldFromJsonString("/Mail Code",properties).getOrElse("")
    )
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .headOption
      .getOrElse("")
      .trim
      .toLowerCase
  }

  private def getPossibleAgentId(row:Row, agentIdMapByEmail: Map[String,String]): String = {
    val email = getPossibleEmail(row)
    val agentId = row.getAs[String](DelightedSurveyResponseHelper.AGENT_ID)
    agentId match {
      case "" | null => agentIdMapByEmail.getOrElse(email,"")
      case _ => agentId
    }
  }

  private def mGetAgentIdFromEmail(rvaEmails: Seq[String], client: DataMappingClient): Map[String, String] = {
    if (rvaEmails.nonEmpty)
      client.mGetUserByEmail(rvaEmails).map { case (email, user) =>
        val teamId = user.username
        email -> teamId
      }
    else Map.empty
  }

  private def getPossibleTeamName(row: Row): String = {
    val properties = row.getAs[String](DelightedSurveyResponseHelper.PROPERTIES)
    Seq(
      getFieldFromJsonString("/Rva_team",properties).getOrElse(""),
      getFieldFromJsonString("/rva_team",properties).getOrElse(""),
      getFieldFromJsonString("/Team",properties).getOrElse(""),
      getTeamNameFromArea(getPossibleArea(row))
    )
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .headOption
      .getOrElse("")
      .trim
      .toUpperCase
      .replaceAll("\\s+","")
  }

  private def getPossibleTeamIds(row: Row, teamIdMapByName: Map[String,String], teamIdMapByAgent: Map[String,String], agentId: String): String = {
    val teamId = row.getAs[String](DelightedSurveyResponseHelper.TEAM_ID)
    val teamName = getPossibleTeamName(row)

      Seq(
      teamId,
      teamIdMapByName.getOrElse(teamName,""),
      teamIdMapByAgent.getOrElse(agentId,"")
    )
      .filterNot(_.isEmpty)
      .filterNot(_ == null)
      .headOption
      .getOrElse("")
  }

  private def mGetTeamIdFromAgent(
  agentIds: Seq[String],
  client: DataMappingClient
  ): Map[String, String] = {
    if (agentIds.nonEmpty)
    client.mGetUser(agentIds).map { case (agentId, user) =>
        val teamId = user.teamId("")
        agentId -> teamId
      }
    else Map.empty
  }
  private def getPossibleArea(row: Row): String = {
    val properties = row.getAs[String](DelightedSurveyResponseHelper.PROPERTIES)
    Seq(
      getFieldFromJsonString("/area",properties).getOrElse(""),
      getFieldFromJsonString("/Area",properties).getOrElse("")
    )
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .headOption
      .getOrElse("")
      .trim
      .toUpperCase
      .replaceAll("\\s+","")
  }

  private def getPossibleMcNames(row: Row, teamName: String): String = {
    val properties = row.getAs[String](DelightedSurveyResponseHelper.PROPERTIES)
    val mcName = getMcNameFromArea(getPossibleArea(row))

    Seq(
      getFieldFromJsonString("/Market Center",properties).getOrElse("").trim.toUpperCase,
      getFieldFromJsonString("/Maket Center",properties).getOrElse("").trim.toUpperCase,
      getFieldFromJsonString("/MC",properties).getOrElse("").trim.toUpperCase,
      getMcNameFromTeamName(teamName),
      mcName
    )
      .filterNot(_.isEmpty)
      .filterNot(_ == null)
      .headOption
      .getOrElse("")
      .toUpperCase
      .replaceAll("\\s+","")
  }

  private def getTeamNameFromArea(area: String): String = {
    if(area.matches(teamNameTemplate))
      area
    else ""
  }

   private def getMcNameFromArea(area: String): String = {
    if(area.matches(marketCenterTemplate))
      area
    else if (area.matches(teamNameTemplate)) {
      val teamName = area
      getMcNameFromTeamName(teamName)
    }
    else ""
  }

  private def getMcNameFromTeamName(teamName: String): String = {
    val matcher = marketCenterPattern.matcher(teamName)
    if (matcher.find())
      matcher.group("mc")
    else ""
  }

  private def getPossibleMcId(row: Row, mcIdMapByName: Map[String,String], mcIdMapByAgent: Map[String,String], agentId: String, mcName: String): String = {
    val mcId = row.getAs[String](DelightedSurveyResponseHelper.MARKET_CENTER_ID)
    Seq(
      mcId,
      mcIdMapByName.getOrElse(mcName,""),
      mcIdMapByAgent.getOrElse(agentId,"")
    )
      .filterNot(_.isEmpty)
      .filterNot(_ == null)
      .headOption
      .getOrElse("")
  }

  private def mGetMcIdFromAgent(
                                   agentIds: Seq[String],
                                   client: DataMappingClient
                                 ): Map[String, String] = {
    if (agentIds.nonEmpty)
      client.mGetUser(agentIds).map { case (agentId, user) =>
        val marketCenterId = user.marketCenterId
        agentId -> marketCenterId
      }
    else Map.empty
  }

  private def mGetNameFromId(ids: Seq[Int], client: DataMappingClient): Map[String, String] = {
    if (ids.nonEmpty){
      client.mGetTeam(ids).map{
        case (id, team) =>
          id.toString -> team.name
      }
    }
    else Map.empty
  }

  private def mGetIdFromName(names: Seq[String], client: DataMappingClient): Map[String, String] = {
    if (names.nonEmpty){
      client.mGetTeamByName(names).map{
        case (name, team) =>
          name -> team.teamId("")
      }
    }
    else Map.empty
  }

  private def getPossibleBusinessUnit(
                                       row: Row,
                                       businessUnit: String,
                                       marketCenterId: String,
                                       mcNameMap: Map[String,String],
                                       teamNameMap: Map[String,String],
                                       teamId: String,
                                       pipelineId: String,
                                       config: Config,
                                     ): String = {
    val properties = row.getAs[String](DelightedSurveyResponseHelper.PROPERTIES)
    Seq(
      businessUnit,
      getFieldFromJsonString("/business_unit",properties).getOrElse(""),
      getBusinessUnitFromMcTeamId(row,marketCenterId,mcNameMap,teamNameMap,teamId),
      detectBusinessUnitFromPipelineId(pipelineId,config)
    )
      .filterNot(_.isEmpty)
      .filterNot(_ == null)
      .headOption
      .getOrElse("")
      .toLowerCase
  }

  private def getBusinessUnitFromMcTeamId(row: Row, marketCenterId: String, mcNameMap: Map[String,String], teamNameMap: Map[String,String], teamId: String): String = {
    val teamName = getPossibleTeamName(row)
    val mcName = getPossibleMcNames(row, teamName)
    val mcTeamName = Seq(
      teamName,
      mcName,
      mcNameMap.getOrElse(marketCenterId,""),
      teamNameMap.getOrElse(teamId,"")
    )
      .filterNot(_.isEmpty)
      .filterNot(_ == null)
      .headOption
      .getOrElse("")
      .toUpperCase

    detectBusinessUnit(mcTeamName) match{
      case Some(bu) => bu
      case None => ""
    }
  }

  private def detectBusinessUnit(name: String): Option[String] = {
    name match {
      case name if name.toUpperCase.startsWith("P") && (name.matches(marketCenterTemplate) || name.matches(teamNameTemplate)) => Some("primary")
      case name if name.toUpperCase.startsWith("A") && (name.matches(marketCenterTemplate) || name.matches(teamNameTemplate))  => Some("secondary")
      case _                                        => None
    }
  }

  def detectBusinessUnitFromPipelineId(pipelineId: String, config: Config): String = {
    val primaryPipelineIds = config.getList("primary_pipeline_ids", ",").asScala.toSet
    val secondaryPipelineIds = config.getList("secondary_pipeline_ids", ",").asScala.toSet

    pipelineId match {
      case pipelineId if primaryPipelineIds.contains(pipelineId)   => "primary"
      case pipelineId if secondaryPipelineIds.contains(pipelineId) => "secondary"
      case _                                                       => ""
    }
  }

  private def mGetPipelineIdFromOppo(oppoIds: Seq[String],client: DataMappingClient): Map[String, String] = {
    if (oppoIds.nonEmpty) {
      Map.empty
    } else Map.empty
  }

  private def standardizedPhase(row: Row, mapping: Mapping): String = {
    val phase = row.getAs[String](DelightedSurveyResponseHelper.PHASE).trim.toLowerCase
    mapping.phaseStandardizedMapping.getOrElse(phase,phase)
  }

  private def mappingStage(phase: String, mapping: Mapping): String = {
    val stage = mapping.phaseToStageMapping.getOrElse(phase,"")
    stage match{
      case stage if stage.nonEmpty => stage
      case _ => ""
    }
  }

  private def getPossibleInteractionTime(row: Row): Long = {
    val sentTime = row.getAs[Long](DelightedSurveyResponseHelper.SENT_TIME)
    val properties = row.getAs[String](DelightedSurveyResponseHelper.PROPERTIES)
    val interactionTime = getFieldFromJsonString("/interaction_time",properties).getOrElse("")
    val interactionDay = getFieldFromJsonString("/interaction_day",properties).getOrElse("")
    val interactionMonth = getFieldFromJsonString("/interaction_month",properties).getOrElse("")
    val interactionYear = getFieldFromJsonString("/interaction_year",properties).getOrElse("")
    sentTime match {
      case 0 => getSentTime(interactionDay, interactionMonth, interactionYear, interactionTime)
      case _ => sentTime
    }
  }

  def getSentTime(interactionDay: String, interactionMonth: String, interactionYear: String, interactionTime: String): Long = {
    val interactionDate = getInteractionDate(interactionDay, interactionMonth, interactionYear, interactionTime)
    interactionDate match {
      case null | "unknown" | "" => 0L
      case _ =>
        Seq(
          scala.util.Try(TimestampUtils.parseMillsFromString(interactionDate, "dd/MM/yyyy")).toOption,
          scala.util.Try(TimestampUtils.parseMillsFromString(interactionDate, "dd/MMM/yyyy")).toOption,
          scala.util.Try(TimestampUtils.parseMillsFromString(interactionDate, "MMM/dd/yyyy")).toOption
        ).flatten.headOption.getOrElse(0L)
    }
  }

  def getInteractionDate(interactionDay: String, interactionMonth: String, interactionYear: String, interactionTime: String):String = {
    val interactionDateArr = normalizedDateFormat(interactionTime).split("/")
    val interactionDate = InteractionDate(interactionDateArr(0), interactionDateArr(1), interactionDateArr(2))

    var month = interactionMonth match{
      case ""|null => interactionDate.month
      case _ => interactionMonth
    }
    var day = interactionDay match{
      case ""|null => interactionDate.day
      case _ => interactionDay
    }
    if (month.matches("\\d+")){
      if (month.toInt > 12) {
        val temp = month
        month = day
        day = temp
      }
    }
    val year = interactionYear match{
      case ""|null => interactionDate.year
      case _ => interactionYear
    }
    s"$day/$month/$year"

  }

  def normalizedDateFormat(rawDate: String): String = {
    val date = rawDate.trim.toLowerCase
      .replaceAll("t","")
      .replaceAll("-","/")
      .replaceAll("\\s+","")
    date match{
      case _ if date.matches("\\d+/\\d+") => "01/".concat(date)
      case _ if date.matches("\\d+/\\d+/\\d+") => date
      case "" => "\"\"/\"\"/\"\""
      case _ => date
    }
  }

  private def getPCid(sentInfo: String, agentId: String): String = {
    ""
  }

  private def getCid(sentInfo: String, agentId: String): String = {
    ""
  }
}
