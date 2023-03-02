package rever.etl.support.cs2_call_cta_analysis
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import rever.rsparkflow.spark.FlowMixin
import rever.rsparkflow.spark.api.annotation.{Output, Table}
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.client.DataMappingClient
import rever.etl.support.cs2_call_cta_analysis.reader.CallCtaEventReader
import rever.etl.support.cs2_call_cta_analysis.writer.CallCtaEventWriter


class CallCtaEvent extends FlowMixin {
  @Output(writer = classOf[CallCtaEventWriter])
  @Table("call_cta_events")
  def build(
      @Table(
        name = "raw_call_cta_events",
        reader = classOf[CallCtaEventReader]
      ) rawCallCtaEventDf: DataFrame,
      config: Config
  ): DataFrame = {

    val df = rawCallCtaEventDf
      .mapPartitions(rows => enhanceSourceCode(rows.toSeq, config, CallCtaHelper.callCtaEventSchema).toIterator)(
        RowEncoder(CallCtaHelper.callCtaEventSchema)
      )
      .persist(StorageLevel.MEMORY_AND_DISK_2)

    df
  }

  private def enhanceSourceCode(rows: Seq[Row], config: Config, schema: StructType): Seq[Row] = {

    val userIds = rows.map(_.getAs[String]("user_id")).filterNot(_ == null).filterNot(_.isEmpty).distinct
    val listingAndProjectAliases = rows
      .filter(row => row.getAs[String]("source_type") != CallCtaHelper.SOURCE_TYPE_PROFILE)
      .map(row => row.getAs[String]("alias"))
      .distinct

    val aliasToSourceCodeMap = mGetIdByAlias(listingAndProjectAliases, config)
    val userIdToPhoneNumberMap = mGetNumberPhoneByUserId(userIds, config)

    rows.map(row => {
      val timestamp = row.getAs[Long]("timestamp")
      val messageId = row.getAs[String]("message_id")
      val anonymousId = row.getAs[String]("anonymous_id")
      val userId = row.getAs[String]("user_id")
      val url = row.getAs[String]("url")
      val sourceType = row.getAs[String]("source_type")
      val alias = row.getAs[String]("alias")
      val event = row.getAs[String]("event")
      val os = row.getAs[String]("ua_os")
      val platform = row.getAs[String]("ua_platform")

      val sourceCode = sourceType match {
        case CallCtaHelper.SOURCE_TYPE_PROFILE => s"$alias@rever.vn"
        case _                                 => aliasToSourceCodeMap.getOrElse(alias, "")
      }
      val userPhoneNumber = userIdToPhoneNumberMap.getOrElse(userId, "")

      new GenericRowWithSchema(
        Array[Any](
          timestamp,
          messageId,
          anonymousId,
          userId,
          userPhoneNumber,
          url,
          sourceType,
          sourceCode,
          "",
          event,
          os,
          platform
        ),
        schema
      )
    })
  }

  private def mGetIdByAlias(aliases: Seq[String], config: Config): Map[String, String] = {
    val client = DataMappingClient.client(config)
    if (aliases.nonEmpty) {
      client.mGetIdFromAlias(aliases)
    } else {
      Map.empty
    }
  }

  def mGetNumberPhoneByUserId(userIds: Seq[String], config: Config): Map[String, String] = {
    val client = DataMappingClient.client(config)
    val userIdMap = client.mGetFrontendUserProfiles(userIds)
    userIdMap.map { case (userId, userProfileJson) =>
      val phoneNumber = userProfileJson.at("/phone_number").asText("")
      userId -> phoneNumber
    }
  }

}
