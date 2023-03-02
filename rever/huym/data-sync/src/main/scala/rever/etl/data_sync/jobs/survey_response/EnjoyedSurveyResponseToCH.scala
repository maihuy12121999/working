package rever.etl.data_sync.jobs.survey_response

import com.fasterxml.jackson.databind
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.codehaus.jackson.JsonNode
import rever.etl.data_sync.jobs.survey_response.EnjoyedSurveyResponseHelper._
import rever.etl.data_sync.jobs.survey_response.reader.EnjoyedSurveyResponseReader
import rever.etl.data_sync.jobs.survey_response.writer.EnjoyedSurveyResponseWriter
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.client.DataMappingClient
import rever.etl.rsparkflow.utils.JsonUtils

import scala.collection.mutable
import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter

class EnjoyedSurveyResponseToCH extends FlowMixin {

  @Output(writer = classOf[EnjoyedSurveyResponseWriter])
  @Table("enjoyed_survey_response")
  def build(
      @Table(
        name = "enjoyed_survey",
        reader = classOf[EnjoyedSurveyResponseReader]
      ) enjoyedSurveyDf: DataFrame,
      config: Config
  ): DataFrame = {

    val df = enjoyedSurveyDf
      .withColumn(
        EnjoyedSurveyResponseHelper.NOTES,
        from_json(col(EnjoyedSurveyResponseHelper.NOTES), ArrayType(StringType)).cast(ArrayType(StringType))
      )
      .withColumn(
        EnjoyedSurveyResponseHelper.TAGS,
        from_json(col(EnjoyedSurveyResponseHelper.TAGS), ArrayType(StringType)).cast(ArrayType(StringType))
      )
      .withColumn(
        EnjoyedSurveyResponseHelper.RESPONSES,
        from_json(col(EnjoyedSurveyResponseHelper.RESPONSES), ArrayType(StringType)).cast(ArrayType(StringType))
      )
      .withColumn("arr_score", regexp_replace(col("arr_score"), "[\\[\\s\\]]", ""))
      .withColumn(
        "list_first_submit_time",
        split(regexp_replace(col("list_first_submit_time"), "[\\[\\s\\]]", ""), ",")
      )
      .withColumn(
        EnjoyedSurveyResponseHelper.RVA_EMAIL,
        EnjoyedSurveyResponseHelper
          .getFieldFromJsonUdf(lit("/rva_email"), col(EnjoyedSurveyResponseHelper.PROPERTIES))
      )
      .withColumn(
        EnjoyedSurveyResponseHelper.CONTACT,
        EnjoyedSurveyResponseHelper
          .getFieldFromJsonUdf(lit("/send_via_sms"), col(EnjoyedSurveyResponseHelper.SENT_INFO))
      )
      .mapPartitions(rows => rows.toSeq.grouped(500).flatMap(enhanceData(_, config, surveyResponseSchema)))(
        RowEncoder(surveyResponseSchema)
      )
      .withColumn(
        EnjoyedSurveyResponseHelper.LOG_TIME,
        lit(System.currentTimeMillis())
      )

    df.printSchema()

    df
  }

  private def enhanceData(rows: Seq[Row], config: Config, schema: StructType): Seq[Row] = {
    val client = DataMappingClient.client(config)
    val mapping = Mapping(
      JsonUtils.fromJson[Map[String,String]](config.getAndDecodeBase64("phase_standardized_mapping")),
      JsonUtils.fromJson[Map[String,String]](config.getAndDecodeBase64("phase_to_stage_mapping")),
      JsonUtils.fromJson[Map[String,Seq[String]]](config.getAndDecodeBase64("oppo_stage_to_phase_id_mapping")),
      JsonUtils.fromJson[Map[String,String]](config.getAndDecodeBase64("business_mapping"))
    )

    val rvaEmails = rows
      .map(_.getAs[String](EnjoyedSurveyResponseHelper.RVA_EMAIL))
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .map(_.trim.toLowerCase)
      .distinct

    val teamNames = rows
      .map(row => getPossibleTeam(row))
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .filterNot(_.matches("\\d+"))
      .map(_.trim.toUpperCase)
      .distinct

    val teamIdMapByName = mGetIdFromName(teamNames, client)
    val teamIdMapByAgentEmail = mGetTeamIdFromAgentEmail(rvaEmails, client)

    val teamIds = rows
      .map(row => {
        getTeamId(row, teamIdMapByName, teamIdMapByAgentEmail)
      })
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .map(_.trim)
      .distinct

    val teamNameMap = mGetNameFromId(teamIds,client)

    val marketCenterNames = rows
      .map(row =>{
        val teamId = getTeamId(row, teamIdMapByName, teamIdMapByAgentEmail)
        getMcName(row, teamNameMap, teamId)
      })
      .filterNot(_.isEmpty)
      .filterNot(_ == null)
      .distinct

    val marketCenterIdMapByName = mDetectMarketCenterId(marketCenterNames, client)
    val marketCenterIdMapByEmail = mGetMcIdFromAgentEmail(rvaEmails, client)

    val marketCenterIds = rows
      .map(row =>{
        val teamId = getTeamId(row, teamIdMapByName, teamIdMapByAgentEmail)
        val mcName = getMcName(row, teamNameMap, teamId)
        getMcId(row, marketCenterIdMapByEmail, marketCenterIdMapByName, mcName)
      })
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .filter(_.matches("\\d+"))
      .distinct

    val phaseIds = rows
      .map(row => getFieldFromJsonString("/oppo_phase_id", row.getAs[String](EnjoyedSurveyResponseHelper.PROPERTIES)))
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .filter(_.matches("\\d+"))
      .map(_.trim.toLong)
      .distinct

    val oppoIds = rows
      .map(row => getFieldFromJsonString("/id_co_hoi", row.getAs[String](EnjoyedSurveyResponseHelper.PROPERTIES)))
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .map(_.trim)
      .distinct

    val pipelineIdMapByPhase = mGetPipelineIdFromPhaseId(phaseIds, client)
    val pipelineIdMapByOppo = mGetPipelineIdFromOppo(oppoIds, client)
    val agentIdMap = mGetAgentIds(rvaEmails, client)

    val mcTeamNameMap = mGetMcTeamNames(teamIds, marketCenterIds, client)

    rows.map(row => {
      val surveySystem = row.getAs[String](EnjoyedSurveyResponseHelper.SURVEY_SYSTEM)
      val surveyId = row.getAs[String](EnjoyedSurveyResponseHelper.SURVEY_ID).trim
      val publicSurveyId = row.getAs[String](EnjoyedSurveyResponseHelper.PUBLIC_SURVEY_ID).trim
      val projectId = row.getAs[String](EnjoyedSurveyResponseHelper.PROJECT_ID).trim
      val channel = row.getAs[String](EnjoyedSurveyResponseHelper.CHANNEL).trim.toLowerCase
      val surveyType = row.getAs[String](EnjoyedSurveyResponseHelper.SURVEY_TYPE).trim.toLowerCase
      var phase = row.getAs[String](EnjoyedSurveyResponseHelper.PHASE).trim.toLowerCase
      var pipelineId = row.getAs[String](EnjoyedSurveyResponseHelper.PIPELINE_ID).trim
      var businessUnit = row.getAs[String](EnjoyedSurveyResponseHelper.BUSINESS_UNIT).trim.toLowerCase
      var cid = row.getAs[String](EnjoyedSurveyResponseHelper.CID).trim
      var pCid = row.getAs[String](EnjoyedSurveyResponseHelper.P_CID).trim
      val score = row.getAs[Int](EnjoyedSurveyResponseHelper.SCORE)
      val comment = row.getAs[String](EnjoyedSurveyResponseHelper.COMMENT).trim
      val properties = row.getAs[String](EnjoyedSurveyResponseHelper.PROPERTIES).trim
      val notes = row.getAs[mutable.WrappedArray[String]](EnjoyedSurveyResponseHelper.NOTES).toArray
      val tags = row.getAs[mutable.WrappedArray[String]](EnjoyedSurveyResponseHelper.TAGS).toArray
      val sentInfo = row.getAs[String](EnjoyedSurveyResponseHelper.SENT_INFO).trim
      val lastResponse = row.getAs[String](EnjoyedSurveyResponseHelper.LAST_RESPONSE).trim
      val responses = row.getAs[mutable.WrappedArray[String]](EnjoyedSurveyResponseHelper.RESPONSES).toArray
      val lastAccessClientInfo = row.getAs[String](EnjoyedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO).trim
      val lastAccessClientInfoPerChannel =
        row.getAs[String](EnjoyedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO_PER_CHANNEL).trim
      val createdTime = row.getAs[Long](EnjoyedSurveyResponseHelper.CREATED_TIME)
      val updatedTime = row.getAs[Long](EnjoyedSurveyResponseHelper.UPDATED_TIME)
      val sentTime = row.getAs[Long](EnjoyedSurveyResponseHelper.SENT_TIME)
      val listFirstSubmitTime = row.getAs[mutable.WrappedArray[String]]("list_first_submit_time")
      val lastSubmitTime = row.getAs[Long](EnjoyedSurveyResponseHelper.LAST_SUBMIT_TIME)
      val rvaEmail = row.getAs[String](EnjoyedSurveyResponseHelper.RVA_EMAIL).trim.toLowerCase
      val totalAccess = row.getAs[Int](EnjoyedSurveyResponseHelper.TOTAL_ACCESS)
      val totalResponse = row.getAs[Int](EnjoyedSurveyResponseHelper.TOTAL_RESPONSE)

      val phaseId = getFieldFromJsonString("/oppo_phase_id", properties).trim

      pipelineId = pipelineId match {
        case "" | null =>
          val getPipelineIdByPhase = pipelineIdMapByPhase.getOrElse(phaseId, "")
           getPipelineIdByPhase match {
            case pipelineId => pipelineId
            case _          =>
              val oppoId = getFieldFromJsonString("/id_co_hoi", properties)
              pipelineIdMapByOppo.getOrElse(oppoId, "")
          }
        case _ => pipelineId
      }

      phase = mappingPhase(phase, mapping)

      val stage = mappingStage(phase, phaseId, mapping)

      val teamId = getTeamId(row, teamIdMapByName, teamIdMapByAgentEmail)

      val marketCenterName = getMcName(row, teamNameMap, teamId)
      val marketCenterId = getMcId(row,marketCenterIdMapByEmail, marketCenterIdMapByName, marketCenterName)

      businessUnit = businessUnit match {
        case "" | null =>
          detectBusinessUnitFromTeamMc(row, teamNameMap, marketCenterId, teamId, mcTeamNameMap) match {
            case Some(businessUnit) => businessUnit
            case None               => detectBusinessUnitFromPipelineId(pipelineId, config)
          }
        case _ => mapping.businessMapping.getOrElse(businessUnit,businessUnit)
      }

      val agentId = agentIdMap.getOrElse(rvaEmail, "")

      cid = cid match {
        case null | "" => ""
        case _         => cid
      }

      pCid = pCid match {
        case null | "" => ""
        case _         => pCid
      }

      val totalResponseScore = row
        .getAs[String]("arr_score")
        .split(",")
        .map(_.toInt)
        .sum

      val firstSubmitTime = listFirstSubmitTime
        .map(_.toLong)
        .min

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

  private def mappingPhase(phase: String, mapping: Mapping): String = {
    mapping.phaseStandardizedMapping.getOrElse(phase,phase)
  }

  private def mappingStage(phase: String, phaseId: String,  mapping: Mapping): String = {
    phase match{
      case phase if phase.nonEmpty && phase != null =>
        val stage = mapping.phaseToStageMapping.getOrElse(phase,"")
        stage match{
          case stage if stage.nonEmpty => stage
          case _ => mappingStageByPhaseId(phaseId, mapping)

        }
      case _ => mappingStageByPhaseId(phaseId, mapping)
    }
  }

  private def mappingStageByPhaseId(phaseId: String, mapping: Mapping): String = {
    phaseId match{
      case "" | null => ""
      case _ =>
        val stageToPhaseMapping = enhanceStageToPhaseMapping(mapping)
        val stageToPhaseJson = JsonUtils.toJson(stageToPhaseMapping)
        JsonUtils.toJsonNode(stageToPhaseJson).at(s"/$phaseId").asText("")
    }
  }

  private def enhanceStageToPhaseMapping(mapping: Mapping): mutable.Map[String,String] = {
    val stageToPhaseMapping: mutable.Map[String,String] = mutable.Map.empty[String,String]
    mapping.oppoStageToPhaseIdMapping
      .map(m =>{
        m._2.map(phaseId => {
          stageToPhaseMapping += (phaseId -> m._1)
      })
    })
    stageToPhaseMapping
  }

  private def getTeamId(row: Row, teamIdMapByName: Map[String, String],teamIdMapByAgentEmail: Map[String,String]): String = {
    val rvaEmail = row.getAs[String](EnjoyedSurveyResponseHelper.RVA_EMAIL).trim
    val team = getPossibleTeam(row)

    team match {
      case teamId if team.matches("\\d+")                    => teamId
      case teamName if teamName.nonEmpty && teamName != null =>
        teamIdMapByName.getOrElse(teamName, "")
      case _ => teamIdMapByAgentEmail.getOrElse(rvaEmail, "")
    }

  }

  private def getPossibleTeam(row: Row): String = {
    Seq(
      getFieldFromJsonString("/rva_team_id", row.getAs[String](EnjoyedSurveyResponseHelper.PROPERTIES)),
      getFieldFromJsonString("/rva_team", row.getAs[String](EnjoyedSurveyResponseHelper.PROPERTIES))
    )
      .filterNot(_.isEmpty)
      .filterNot(_ == null)
      .headOption
      .getOrElse("")
      .trim
      .toUpperCase
  }

  private def mGetTeamIdFromAgentEmail(
      rvaEmails: Seq[String],
      client: DataMappingClient
  ): Map[String, String] = {
    if (rvaEmails.nonEmpty)
      client.mGetUserByEmail(rvaEmails).map { case (email, user) =>
        val teamId = user.teamId("")
        email -> teamId
      }
    else Map.empty
  }

  private def mGetIdFromName(
  teamNames: Seq[String],
  client: DataMappingClient
  ): Map[String, String] = {
    if (teamNames.nonEmpty)
      client.mGetTeamByName(teamNames).map { case (teamName, team) =>
        val teamId = team.teamId("")
        teamName -> teamId
      }
    else Map.empty
  }

  private def mGetNameFromId(
  teamIds: Seq[String],
  client: DataMappingClient
  ): Map[String, String] = {
    if (teamIds.nonEmpty)
      client.mGetTeam(teamIds.map(_.toInt)).map { case (teamId, team) =>
        val teamName = team.name
        teamId.toString -> teamName
      }
    else Map.empty
  }

  private def mGetMcTeamNames(teamIds: Seq[String], marketCenterIds: Seq[String], client: DataMappingClient): Map[String, String] = {
    val ids = marketCenterIds ++ teamIds
    if (ids.nonEmpty) {
      client
        .mGetTeam(ids.map(_.toInt))
        .map { case (teamId, team) =>
          teamId.toString -> team.name
        }
    } else Map.empty
  }

  private def mGetMcNamesFromTeamId(teamIds: Seq[String], client: DataMappingClient): Map[String, String] = {
    if (teamIds.nonEmpty) {
      client
        .mGetTeam(teamIds.map(_.toInt))
        .map { case (teamId, team) =>
          teamId.toString -> getMcNameFromTeamName(team.name)
        }
    } else Map.empty
  }

  private def getMcNameFromTeamName(teamName: String): String = {
    val matcher = marketCenterPattern.matcher(teamName)
    if (matcher.find())
      matcher.group("mc")
    else ""
  }

  private def getMcName(
                         row: Row,
                         teamNameMap: Map[String, String],
                         teamId: String
                   ): String = {
    val mc = row.getAs[String](EnjoyedSurveyResponseHelper.MARKET_CENTER_ID).trim.toUpperCase
    val area = getFieldFromJsonString("/area", row.getAs[String](PROPERTIES)).trim.toUpperCase
    val teamName = Seq(
      teamNameMap.getOrElse(teamId,""),
      getPossibleTeam(row)
    )
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .filterNot(_.matches("\\d+"))
      .headOption
      .getOrElse("")
      .toUpperCase
      .trim

    val mcName = getMcNameFromTeamName(teamName)

     mc match{
       case mcName if mcName.nonEmpty && mcName != null && !mcName.matches("\\d+") =>
         mcName
       case _ =>
         mcName match{
           case "" | null =>
             if (area.matches(marketCenterTemplate))
               area
             else ""
           case _ => mcName
         }
     }
  }

  private def getMcId(
                       row: Row,
                       marketCenterIdMapByEmail: Map[String,String],
                       marketCenterIdMapByName: Map[String,String],
                       mcName: String
                     ): String = {
    val rvaEmail = row.getAs[String](RVA_EMAIL)
    val mcId = row.getAs[String](MARKET_CENTER_ID)
    mcId match{
      case "" | null =>
        marketCenterIdMapByName.getOrElse(mcName,"") match {
          case mcId if mcId.nonEmpty && mcId != null => mcId
          case _ => marketCenterIdMapByEmail.getOrElse(rvaEmail,"")
        }
      case _ => mcId
    }
  }

  def mDetectMarketCenterId(marketCenterNames: Seq[String], client: DataMappingClient): Map[String, String] = {
    if (marketCenterNames.nonEmpty) {
      client.mGetTeamByName(marketCenterNames).map { case (mcName, mc) =>
        mcName -> mc.teamId("")
      }
    } else
      Map.empty
  }

  private def mGetMcIdFromAgentEmail(
      rvaEmails: Seq[String],
      client: DataMappingClient
  ): Map[String, String] = {
    if (rvaEmails.nonEmpty)
      client.mGetUserByEmail(rvaEmails).map { case (email, user) =>
        val marketCenterId = user.marketCenterId
        email -> marketCenterId
      }
    else Map.empty
  }

  def detectBusinessUnitFromTeamMc(row: Row, teamNameMap: Map[String,String], marketCenterId: String, teamId: String, mcTeamNameMap: Map[String,String]): Option[String] = {
    val teamMcName = getPossibleTeamMcName(row,teamNameMap,marketCenterId,teamId, mcTeamNameMap)

    getBusinessUnit(teamMcName)
  }

  def getPossibleTeamMcName(row: Row, teamNameMap: Map[String,String], marketCenterId: String, teamId: String, mcTeamNameMap: Map[String,String]): String = {
    val teamMcId = Seq(marketCenterId,teamId)
      .filterNot(_ == null)
      .filterNot(_.trim.isEmpty)
      .map(_.trim)
      .headOption
      .getOrElse("")

    Seq(
      mcTeamNameMap.getOrElse(teamMcId,""),
      getMcName(row, teamNameMap, teamId)
    )
      .filterNot(_ == null)
      .filterNot(_.trim.isEmpty)
      .map(_.trim)
      .headOption
      .getOrElse("")
  }

  private def getBusinessUnit(name: String): Option[String] = {
    name match{
      case name if name.toUpperCase.startsWith("P") && (name.matches(teamNameTemplate)||name.matches(marketCenterTemplate))  => Some("primary")
      case name if name.toUpperCase.startsWith("A") && (name.matches(teamNameTemplate)||name.matches(marketCenterTemplate)) => Some("secondary")
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

  private def mGetAgentIds(
      rvaEmails: Seq[String],
      client: DataMappingClient
  ): Map[String, String] = {
    if (rvaEmails.nonEmpty)
      client.mGetUserByEmail(rvaEmails).map { case (email, user) =>
        val agentId = user.username
        email -> agentId
      }
    else Map.empty
  }

  private def mGetPipelineIdFromPhaseId(
      phaseIds: Seq[Long],
      client: DataMappingClient
  ): Map[String, String] = {
    if (phaseIds.nonEmpty)
      client.mGetSalePipelinePhases(phaseIds).map { case (phaseId, user) =>
        val pipelineId = user.pipelineId
        phaseId.toString -> pipelineId.toString
      }
    else Map.empty
  }

  private def mGetPipelineIdFromOppo(
      oppoIds: Seq[String],
      client: DataMappingClient
  ): Map[String, String] = {
    if (oppoIds.nonEmpty) {
      Map.empty
    } else Map.empty
  }
}
