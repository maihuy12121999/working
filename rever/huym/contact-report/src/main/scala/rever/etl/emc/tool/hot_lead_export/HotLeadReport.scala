package rever.etl.emc.tool.hot_lead_export

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.storage.StorageLevel
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.domain.SparkSessionImplicits._
import rever.etl.emc.tool.hot_lead_export.domain.HotLeadFields
import rever.etl.emc.tool.hot_lead_export.reader.{CreatedPContactReader, CurrentUserReader, HistoricalCallReader, PContactDMReader}
import rever.etl.emc.tool.hot_lead_export.writer.HotLeadWriter

import scala.language.postfixOps
class HotLeadReport extends FlowMixin {
  @Output(writer = classOf[HotLeadWriter])
  @Table(name = "hot_lead")
  def build(
      @Table(
        name = "created_personal_contact",
        reader = classOf[CreatedPContactReader]
      ) createdPContactDf: DataFrame,
      @Table(
        name = "personal_contact",
        reader = classOf[PContactDMReader]
      ) pContactDf: DataFrame,
      @Table(
        name = "call_historical",
        reader = classOf[HistoricalCallReader]
      ) callHistoricalDf: DataFrame,
      @Table(
        name = "current_a0_users",
        reader = classOf[CurrentUserReader]
      ) currentA0UserDf: DataFrame,
      config: Config
  ): DataFrame = {

    val pContactWithOwnerDf = enhanceContactOwnerInfo(createdPContactDf, currentA0UserDf)

    val df = enhanceLatestPContactInfo(pContactWithOwnerDf, pContactDf)

    val pContactWithCallDf = df
      .joinWith(
        callHistoricalDf,
        df(HotLeadFields.OWNER_ID) === callHistoricalDf(HotLeadFields.AGENT_ID)
          && df(HotLeadFields.PHONE) === callHistoricalDf(HotLeadFields.DESTINATION),
        "left"
      )
      .select(
        col(s"_1.${HotLeadFields.CID}"),
        col(s"_1.${HotLeadFields.SYSTEM_TAGS}"),
        col(s"_1.${HotLeadFields.OWNER_EMAIL}"),
        col(s"_1.${HotLeadFields.TEAM_NAME}"),
        col(s"_1.${HotLeadFields.PHONE}"),
        col(s"_1.${HotLeadFields.OWNER_ID}"),
        col(s"_1.${HotLeadFields.CONTACT_STATUS}"),
        col(s"_1.${HotLeadFields.USER_TAGS}"),
        col(s"_1.${HotLeadFields.TIMESTAMP}"),
        col(s"_2.${HotLeadFields.DURATION}"),
        col(s"_2.${HotLeadFields.CALL_STATUS}"),
        col(s"_2.${HotLeadFields.CALL_TIME}")
      )
      .where(col(HotLeadFields.CALL_TIME) >= col(HotLeadFields.TIMESTAMP))
      .persist(StorageLevel.MEMORY_AND_DISK_2)

    val totalCallDf = pContactWithCallDf
      .groupBy(
        col(HotLeadFields.OWNER_ID),
        col(HotLeadFields.CID),
        col(HotLeadFields.PHONE)
      )
      .agg(count(HotLeadFields.OWNER_ID).as("total_call"))

    val distinctResultDf = pContactWithCallDf.dropDuplicateCols(
      Seq(
        HotLeadFields.OWNER_ID,
        HotLeadFields.CID,
        HotLeadFields.PHONE
      ),
      col(HotLeadFields.CALL_TIME).asc
    )
    pContactWithCallDf.printSchema()
    distinctResultDf.printSchema()
    totalCallDf.printSchema()

    distinctResultDf
      .join(
        totalCallDf,
        distinctResultDf(HotLeadFields.OWNER_ID) === totalCallDf(HotLeadFields.OWNER_ID)
          && distinctResultDf(HotLeadFields.CID) === totalCallDf(HotLeadFields.CID)
          && distinctResultDf(HotLeadFields.PHONE) === totalCallDf(HotLeadFields.PHONE)
      )
      .select(
        distinctResultDf(HotLeadFields.CID),
        distinctResultDf(HotLeadFields.SYSTEM_TAGS),
        distinctResultDf(HotLeadFields.OWNER_EMAIL),
        col(HotLeadFields.TEAM_NAME),
        distinctResultDf(HotLeadFields.TIMESTAMP).as(HotLeadFields.CREATED_TIME),
        distinctResultDf(HotLeadFields.CALL_TIME).as(HotLeadFields.FIRST_CALL_TIME),
        distinctResultDf(HotLeadFields.DURATION),
        distinctResultDf(HotLeadFields.CALL_STATUS),
        distinctResultDf(HotLeadFields.USER_TAGS),
        distinctResultDf(HotLeadFields.CONTACT_STATUS),
        totalCallDf("total_call")
      )
  }

  private def enhanceContactOwnerInfo(createdPContactDf: DataFrame, currentA0UserDf: DataFrame): DataFrame = {
    createdPContactDf
      .joinWith(currentA0UserDf, createdPContactDf(HotLeadFields.OWNER_ID) === currentA0UserDf("username"), "left")
      .select(
        col(s"_1.${HotLeadFields.CID}"),
        col(s"_1.${HotLeadFields.P_CID}"),
        col(s"_1.${HotLeadFields.PHONE}"),
        col(s"_1.${HotLeadFields.OWNER_ID}"),
        col(s"_1.${HotLeadFields.TIMESTAMP}"),
        col(s"_2.work_email").as(HotLeadFields.OWNER_EMAIL),
        col(s"_2.team_name").as(HotLeadFields.TEAM_NAME)
      )
      .na
      .fill("unknown")
      .na
      .replace(
        Seq(
          HotLeadFields.PHONE,
          HotLeadFields.OWNER_EMAIL,
          HotLeadFields.TEAM_NAME
        ),
        Map("" -> "unknown")
      )
  }

  private def enhanceLatestPContactInfo(pContactWithOwnerDf: DataFrame, dmPContactDf: DataFrame): DataFrame = {
    pContactWithOwnerDf
      .joinWith(
        dmPContactDf,
        pContactWithOwnerDf(HotLeadFields.P_CID) === dmPContactDf(HotLeadFields.P_CID),
        "inner"
      )
      .select(
        col(s"_1.${HotLeadFields.CID}"),
        col(s"_2.${HotLeadFields.SYSTEM_TAGS}"),
        col(s"_1.${HotLeadFields.OWNER_EMAIL}"),
        col(s"_1.${HotLeadFields.TEAM_NAME}").as(HotLeadFields.TEAM_NAME),
        col(s"_1.${HotLeadFields.PHONE}"),
        col(s"_1.${HotLeadFields.OWNER_ID}"),
        col(s"_2.${HotLeadFields.CONTACT_STATUS}"),
        col(s"_2.${HotLeadFields.USER_TAGS}"),
        col(s"_1.${HotLeadFields.TIMESTAMP}")
      )
  }

}
