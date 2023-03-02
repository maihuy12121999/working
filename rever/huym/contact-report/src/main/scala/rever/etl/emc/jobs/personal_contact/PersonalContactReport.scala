package rever.etl.emc.jobs.personal_contact

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, countDistinct, expr, when}
import org.apache.spark.storage.StorageLevel
import rever.etl.emc.domain.PersonalContactFields
import rever.etl.emc.jobs.personal_contact.reader.{PersonalContactReader, UserHistoricalReader}
import rever.etl.emc.jobs.personal_contact.writer.PersonalContactWriter
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.extensions.{ByIntervalReportTemplate, RapIngestWriterMixin}

/** @author vylnt1
  */
class PersonalContactReport extends FlowMixin with ByIntervalReportTemplate with RapIngestWriterMixin {
  @Output(writer = classOf[PersonalContactWriter])
  @Table(name = "personal_contact")
  def build(
      @Table(
        name = "raw_personal_contact",
        reader = classOf[PersonalContactReader]
      ) rawPersonalContactDf: DataFrame,
      @Table(
        name = "current_a0_users",
        reader = classOf[UserHistoricalReader]
      ) currentA0UserDf: DataFrame,
      config: Config
  ): DataFrame = {

    val reportTime = config.getDailyReportTime

    val dailyDf = normalizeColumns(rawPersonalContactDf, currentA0UserDf)
      .select(
        col(PersonalContactFields.DATE),
        col(PersonalContactFields.BUSINESS_UNIT),
        col(PersonalContactFields.MARKET_CENTER_ID),
        col(PersonalContactFields.TEAM_ID),
        col(PersonalContactFields.USER_TYPE),
        col(PersonalContactFields.AGENT_ID),
        col(PersonalContactFields.STATUS),
        col(PersonalContactFields.P_CID)
      )
      .persist(StorageLevel.MEMORY_AND_DISK_2)

    val previousA0Df = getPreviousA0DfFromS3(config.getJobId, reportTime, config)
      .map(_.persist(StorageLevel.MEMORY_AND_DISK_2))

    val anDf = previousA0Df match {
      case Some(previousA0Df) =>
        dailyDf
          .join(
            previousA0Df,
            dailyDf(PersonalContactFields.P_CID) === previousA0Df(PersonalContactFields.P_CID),
            "leftanti"
          )
          .persist(StorageLevel.MEMORY_AND_DISK_2)
      case None => dailyDf
    }

    val a0Df = previousA0Df match {
      case Some(previousA0Df) =>
        previousA0Df
          .unionByName(dailyDf, allowMissingColumns = true)
          .orderBy(col(PersonalContactFields.DATE).desc)
          .dropDuplicates(PersonalContactFields.P_CID)
          .persist(StorageLevel.MEMORY_AND_DISK_2)
      case None => dailyDf
    }

    exportAnDfToS3(config.getJobId, reportTime, anDf, config)
    exportA0DfToS3(config.getJobId, reportTime, a0Df, config)

    saveNewPersonalContact(anDf, reportTime, config)

    val totalContactResultDf = calcA0ByInterval(
      a0Df,
      reportTime,
      Seq(
        PersonalContactFields.BUSINESS_UNIT,
        PersonalContactFields.MARKET_CENTER_ID,
        PersonalContactFields.TEAM_ID,
        PersonalContactFields.USER_TYPE,
        PersonalContactFields.AGENT_ID,
        PersonalContactFields.STATUS
      ),
      Map(
        countDistinct(col(PersonalContactFields.P_CID)) -> PersonalContactFields.NUM_CONTACT
      ),
      PersonalContactSchema.generateRow,
      config,
      persistDataset = true
    )

    totalContactResultDf
  }

  private def saveNewPersonalContact(anDf: DataFrame, reportTime: Long, config: Config) = {
    val newContactResultDf = calcAnByInterval(
      config.getJobId,
      anDf,
      reportTime,
      Seq(
        PersonalContactFields.BUSINESS_UNIT,
        PersonalContactFields.MARKET_CENTER_ID,
        PersonalContactFields.TEAM_ID,
        PersonalContactFields.USER_TYPE,
        PersonalContactFields.AGENT_ID,
        PersonalContactFields.STATUS
      ),
      Map(
        countDistinct(col(PersonalContactFields.P_CID)) -> PersonalContactFields.NUM_CONTACT
      ),
      PersonalContactSchema.generateRow,
      config,
      exportAnDataset = false,
      persistDataset = true
    )

    val topic = config.get("new_personal_contact_topic")
    ingestDataframe(config, newContactResultDf, topic)(PersonalContactSchema.toNewPersonalContactRecord, 800)

    mergeIfRequired(config, newContactResultDf, topic)

  }

  private def normalizeColumns(rawPersonalContactDf: DataFrame, currentA0UserDf: DataFrame): DataFrame = {
    rawPersonalContactDf
      .joinWith(currentA0UserDf, rawPersonalContactDf("agent_id") === currentA0UserDf("username"), "left")
      .select(
        expr("""(BIGINT(to_timestamp(to_date(timestamp_millis(_1.timestamp),"dd/MM/yyyy")))*1000)""").as(
          PersonalContactFields.DATE
        ),
        col("_2.business_unit"),
        col("_2.market_center_id"),
        col("_2.team_id"),
        col("_2.user_type"),
        col("_1.agent_id"),
        col("_1.status"),
        col("_1.p_cid")
      )
      .withColumn(
        PersonalContactFields.BUSINESS_UNIT,
        when(
          col(PersonalContactFields.BUSINESS_UNIT).isNull.or(col(PersonalContactFields.BUSINESS_UNIT).equalTo("")),
          "unknown"
        )
          .otherwise(col(PersonalContactFields.BUSINESS_UNIT))
      )
      .withColumn(
        PersonalContactFields.MARKET_CENTER_ID,
        when(
          col(PersonalContactFields.MARKET_CENTER_ID).isNull.or(
            col(PersonalContactFields.MARKET_CENTER_ID).equalTo("")
          ),
          "unknown"
        )
          .otherwise(col(PersonalContactFields.MARKET_CENTER_ID))
      )
      .withColumn(
        PersonalContactFields.TEAM_ID,
        when(col(PersonalContactFields.TEAM_ID).isNull.or(col(PersonalContactFields.TEAM_ID).equalTo("")), "unknown")
          .otherwise(col(PersonalContactFields.TEAM_ID))
      )
      .withColumn(
        PersonalContactFields.USER_TYPE,
        when(
          col(PersonalContactFields.USER_TYPE).isNull.or(col(PersonalContactFields.USER_TYPE).equalTo("")),
          "unknown"
        )
          .otherwise(col(PersonalContactFields.USER_TYPE))
      )
      .withColumn(
        PersonalContactFields.AGENT_ID,
        when(col(PersonalContactFields.AGENT_ID).isNull.or(col(PersonalContactFields.AGENT_ID).equalTo("")), "unknown")
          .otherwise(col(PersonalContactFields.AGENT_ID))
      )
  }

}
