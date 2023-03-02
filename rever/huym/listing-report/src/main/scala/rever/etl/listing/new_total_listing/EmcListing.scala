package rever.etl.listing.new_total_listing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import rever.etl.listing.new_total_listing.reader.{EmcListingReader, UserHistoricalReader}
import rever.etl.listing.new_total_listing.writer.EmcListingWriter
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.api.udf.RUdfUtils
import rever.etl.rsparkflow.extensions.{ByIntervalReportTemplate, RapIngestWriterMixin}

class EmcListing extends FlowMixin with ByIntervalReportTemplate with RapIngestWriterMixin {
  @Output(writer = classOf[EmcListingWriter])
  @Table("listing")
  def build(
      @Table(
        name = "listing_action",
        reader = classOf[EmcListingReader]
      ) listingDf: DataFrame,
      @Table(
        name = "current_a0_users",
        reader = classOf[UserHistoricalReader]
      ) currentA0UserDf: DataFrame,
      config: Config
  ): DataFrame = {
    val reportTime = config.getDailyReportTime

    val dailyDf = normalizeColumns(listingDf, currentA0UserDf)
      .drop(EmcListingHelper.MARKET_CENTER_NAME, EmcListingHelper.TEAM_NAME)
      .persist(StorageLevel.MEMORY_AND_DISK_2)

    val previousA0Df = getPreviousA0DfFromS3(config.getJobId, reportTime, config)
      .map(_.persist(StorageLevel.MEMORY_AND_DISK_2))

    val anDf = previousA0Df match {
      case Some(previousA0Df) =>
        dailyDf
          .join(
            previousA0Df,
            dailyDf(EmcListingHelper.LISTING_ID) === previousA0Df(EmcListingHelper.LISTING_ID),
            "leftanti"
          )
          .persist(StorageLevel.MEMORY_AND_DISK_2)
      case None => dailyDf
    }

    val a0Df = previousA0Df match {
      case Some(previousA0Df) =>
        previousA0Df
          .unionByName(dailyDf, allowMissingColumns = true)
          .orderBy(col(EmcListingHelper.DATE).desc)
          .dropDuplicates(EmcListingHelper.LISTING_ID)
          .persist(StorageLevel.MEMORY_AND_DISK_2)
      case None => dailyDf
    }

    exportAnDfToS3(config.getJobId, reportTime, anDf, config)
    exportA0DfToS3(config.getJobId, reportTime, a0Df, config)

    saveNewListing(anDf, reportTime, config)

    val totalListingResultDf = calcA0ByInterval(
      a0Df,
      reportTime,
      Seq(
        EmcListingHelper.LISTING_CHANNEL,
        EmcListingHelper.BUSINESS_UNIT,
        EmcListingHelper.MARKET_CENTER_ID,
        EmcListingHelper.TEAM_ID,
        EmcListingHelper.CITY_ID,
        EmcListingHelper.DISTRICT_ID,
        EmcListingHelper.PROJECT_ID,
        EmcListingHelper.AGENT_ID,
        EmcListingHelper.PUBLISHED_ID,
        EmcListingHelper.SERVICE_TYPE,
        EmcListingHelper.PROPERTY_STATUS,
        EmcListingHelper.PROPERTY_TRANSACTION_STAGE
      ),
      Map(
        countDistinct(col(EmcListingHelper.LISTING_ID)) -> EmcListingHelper.TOTAL_LISTING
      ),
      EmcListingHelper.generateRow,
      config,
      persistDataset = true
    )

    totalListingResultDf
  }

  private def saveNewListing(anDf: DataFrame, reportTime: Long, config: Config): Unit = {
    val newListingResultDf = calcAnByInterval(
      config.getJobId,
      anDf,
      reportTime,
      Seq(
        EmcListingHelper.LISTING_CHANNEL,
        EmcListingHelper.BUSINESS_UNIT,
        EmcListingHelper.MARKET_CENTER_ID,
        EmcListingHelper.TEAM_ID,
        EmcListingHelper.CITY_ID,
        EmcListingHelper.DISTRICT_ID,
        EmcListingHelper.PROJECT_ID,
        EmcListingHelper.AGENT_ID,
        EmcListingHelper.PUBLISHED_ID,
        EmcListingHelper.SERVICE_TYPE,
        EmcListingHelper.PROPERTY_STATUS,
        EmcListingHelper.PROPERTY_TRANSACTION_STAGE
      ),
      Map(
        countDistinct(col(EmcListingHelper.LISTING_ID)) -> EmcListingHelper.TOTAL_LISTING
      ),
      EmcListingHelper.generateRow,
      config,
      exportAnDataset = false,
      persistDataset = true
    )

    val topic = config.get("new_listing_topic")
    ingestDataframe(config, newListingResultDf, topic)(EmcListingHelper.toNewListingRecord, 800)

    mergeIfRequired(config, newListingResultDf, topic)
  }

  private def normalizeColumns(rawListingDf: DataFrame, currentA0UserDf: DataFrame): DataFrame = {
    val seqDimensions = Seq(
      EmcListingHelper.LISTING_CHANNEL,
      EmcListingHelper.BUSINESS_UNIT,
      EmcListingHelper.MARKET_CENTER_ID,
      EmcListingHelper.TEAM_ID,
      EmcListingHelper.CITY_ID,
      EmcListingHelper.DISTRICT_ID,
      EmcListingHelper.PROJECT_ID,
      EmcListingHelper.AGENT_ID,
      EmcListingHelper.PUBLISHED_ID,
      EmcListingHelper.SERVICE_TYPE,
      EmcListingHelper.PROPERTY_STATUS,
      EmcListingHelper.PROPERTY_TRANSACTION_STAGE
    )

    rawListingDf
      .joinWith(currentA0UserDf, rawListingDf("agent_id") === currentA0UserDf("username"), "left")
      .select(
        expr("""(BIGINT(to_timestamp(to_date(timestamp_millis(_1.timestamp),"dd/MM/yyyy")))*1000)""").as(
          EmcListingHelper.DATE
        ),
        col("_1.listing_channel"),
        col("_2.business_unit"),
        when(
          col("_1.market_center_id").isNull
            .or(col("_1.market_center_id").equalTo("")),
          col("_2.market_center_id")
        )
          .otherwise(col("_1.market_center_id"))
          .as(EmcListingHelper.MARKET_CENTER_ID),
        col("_1.market_center_name"),
        when(
          col("_1.team_id").isNull
            .or(col("_1.team_id").equalTo("")),
          col("_2.team_id")
        )
          .otherwise(col("_1.team_id"))
          .as(EmcListingHelper.TEAM_ID),
        col("_1.team_name"),
        col("_1.city_id"),
        col("_1.district_id"),
        col("_1.project_id"),
        col("_1.agent_id"),
        col("_1.published_id"),
        col("_1.service_type"),
        col("_1.property_status"),
        col("_1.property_transaction_stage"),
        col("_1.listing_id")
      )
      .withColumn(
        EmcListingHelper.BUSINESS_UNIT,
        callUDF(
          RUdfUtils.RV_DETECT_BUSINESS_UNIT,
          col(EmcListingHelper.BUSINESS_UNIT),
          col(EmcListingHelper.MARKET_CENTER_NAME),
          col(EmcListingHelper.TEAM_NAME)
        )
      )
      .na
      .fill("unknown")
      .na
      .replace(seqDimensions, Map("" -> "unknown"))
  }
}
