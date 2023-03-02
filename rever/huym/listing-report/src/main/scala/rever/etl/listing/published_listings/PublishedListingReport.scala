package rever.etl.listing.published_listings

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import rever.etl.listing.domain.{PublishTypes, PublishedListingFields}
import rever.etl.listing.published_listings.reader.{CsvListingActionReader, ListingActionReader, UserHistoricalReader}
import rever.etl.listing.published_listings.writer.PublishedListingWriter
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.domain.SparkSessionImplicits._
import rever.etl.rsparkflow.extensions.{ByIntervalReportTemplate, RapIngestWriterMixin}
import rever.etl.rsparkflow.utils.JsonUtils

import scala.concurrent.duration.DurationInt

class PublishedListingReport extends FlowMixin with ByIntervalReportTemplate  with RapIngestWriterMixin{

  @Output(writer = classOf[PublishedListingWriter])
  @Table("published_listings")
  def build(
      @Table(
        name = "raw_listing_action",
        reader = classOf[ListingActionReader]
      ) rawPublishedDf: DataFrame,
      @Table(
        name = "current_a0_users",
        reader = classOf[UserHistoricalReader]
      ) currentA0UserDf: DataFrame,
      config: Config
  ): DataFrame = {
    val reportTime = config.getDailyReportTime

    val dailyDf = enhanceBusinessUnitMCAndTeam(rawPublishedDf, currentA0UserDf).persist(StorageLevel.MEMORY_AND_DISK_2)

    val previousA0Df = buildPreviousA0Df(config).map(_.persist(StorageLevel.MEMORY_AND_DISK_2))
    val firstPublishedDf = buildFirstPublishDf(dailyDf, previousA0Df).persist(StorageLevel.MEMORY_AND_DISK_2)

    val nonFirstPublishedDf = dailyDf
      .join(
        firstPublishedDf,
        dailyDf(PublishedListingFields.PROPERTY_ID) === firstPublishedDf(PublishedListingFields.PROPERTY_ID),
        "leftanti"
      )
      .persist(StorageLevel.MEMORY_AND_DISK_2)

    val republishedNewOwnerDf = buildRePublishNewOwnerDf(
      nonFirstPublishedDf,
      previousA0Df
    ).persist(StorageLevel.MEMORY_AND_DISK_2)

    val republishedAfter30DaysDf = buildRePublishAfter30DayDf(
      nonFirstPublishedDf,
      republishedNewOwnerDf,
      previousA0Df,
      config
    ).persist(StorageLevel.MEMORY_AND_DISK_2)

    val a0Df = buildA0Df(previousA0Df, dailyDf)

    val publishedListingDf = firstPublishedDf
      .unionByName(republishedNewOwnerDf, allowMissingColumns = true)
      .unionByName(republishedAfter30DaysDf, allowMissingColumns = true)
      .select(
        expr(
          s"""BIGINT(to_timestamp(to_date(timestamp_millis(${PublishedListingFields.TIMESTAMP}),"dd/MM/yyyy")))*1000"""
        ).as(PublishedListingFields.DATE),
        col(PublishedListingFields.LISTING_CHANNEL),
        col(PublishedListingFields.PUBLISH_TYPE),
        col(PublishedListingFields.SERVICE_TYPE),
        col(PublishedListingFields.BUSINESS_UNIT),
        col(PublishedListingFields.MARKET_CENTER_ID),
        col(PublishedListingFields.TEAM_ID),
        col(PublishedListingFields.CITY_ID),
        col(PublishedListingFields.DISTRICT_ID),
        col(PublishedListingFields.PROJECT_ID),
        col(PublishedListingFields.AGENT_ID),
        col(PublishedListingFields.USER_TYPE),
        col(PublishedListingFields.PUBLISHER_ID),
        col(PublishedListingFields.CID),
        col(PublishedListingFields.PROPERTY_ID)
      )

    writePublishedListingDataMartToCH(publishedListingDf,config)

    exportAnDfToS3(config.getJobId, reportTime, publishedListingDf, config)
    exportA0DfToS3(config.getJobId, reportTime, a0Df, config)

    val publishedResultDf = calcAnByInterval(
      config.getJobId,
      publishedListingDf,
      reportTime,
      Seq(
        PublishedListingFields.LISTING_CHANNEL,
        PublishedListingFields.PUBLISH_TYPE,
        PublishedListingFields.SERVICE_TYPE,
        PublishedListingFields.BUSINESS_UNIT,
        PublishedListingFields.MARKET_CENTER_ID,
        PublishedListingFields.TEAM_ID,
        PublishedListingFields.CITY_ID,
        PublishedListingFields.DISTRICT_ID,
        PublishedListingFields.PROJECT_ID,
        PublishedListingFields.AGENT_ID,
        PublishedListingFields.USER_TYPE,
        PublishedListingFields.PUBLISHER_ID,
        PublishedListingFields.CID
      ),
      Map(
        countDistinct(col(PublishedListingFields.PROPERTY_ID)) -> PublishedListingFields.TOTAL_LISTINGS,
        count(col(PublishedListingFields.PROPERTY_ID)) -> PublishedListingFields.TOTAL_ACTIONS
      ),
      PublishedListingSchema.generateRow,
      config,
      exportAnDataset = false,
      persistDataset = true
    )



    publishedResultDf
  }

  private def enhanceBusinessUnitMCAndTeam(dailyDf: DataFrame, currentA0UserDf: DataFrame): DataFrame = {

    val df = dailyDf
      .join(currentA0UserDf, dailyDf("agent_id") === currentA0UserDf("username"), "left")
      .select(
        dailyDf(PublishedListingFields.TIMESTAMP),
        dailyDf(PublishedListingFields.LISTING_CHANNEL),
        dailyDf(PublishedListingFields.PROPERTY_ID),
        dailyDf(PublishedListingFields.SERVICE_TYPE),
        currentA0UserDf(PublishedListingFields.BUSINESS_UNIT),
        when(
          dailyDf(PublishedListingFields.MARKET_CENTER_ID).isNull || dailyDf(
            PublishedListingFields.MARKET_CENTER_ID
          ) === lit("") || dailyDf(
            PublishedListingFields.MARKET_CENTER_ID
          ) === lit("unknown"),
          currentA0UserDf(PublishedListingFields.MARKET_CENTER_ID)
        ).otherwise(dailyDf(PublishedListingFields.MARKET_CENTER_ID)).as(PublishedListingFields.MARKET_CENTER_ID),
        when(
          dailyDf(PublishedListingFields.TEAM_ID).isNull || dailyDf(PublishedListingFields.TEAM_ID) === lit(
            ""
          ) || dailyDf(
            PublishedListingFields.TEAM_ID
          ) === lit("unknown"),
          currentA0UserDf(PublishedListingFields.TEAM_ID)
        ).otherwise(dailyDf(PublishedListingFields.TEAM_ID)).as(PublishedListingFields.TEAM_ID),
        dailyDf(PublishedListingFields.CITY_ID),
        dailyDf(PublishedListingFields.DISTRICT_ID),
        dailyDf(PublishedListingFields.PROJECT_ID),
        dailyDf(PublishedListingFields.AGENT_ID),
        currentA0UserDf(PublishedListingFields.USER_TYPE),
        dailyDf(PublishedListingFields.PUBLISHER_ID),
        dailyDf(PublishedListingFields.CID)
      )

    df.na
      .replace(
        Seq(
          PublishedListingFields.BUSINESS_UNIT,
          PublishedListingFields.MARKET_CENTER_ID,
          PublishedListingFields.TEAM_ID,
          PublishedListingFields.CITY_ID,
          PublishedListingFields.DISTRICT_ID,
          PublishedListingFields.PROJECT_ID,
          PublishedListingFields.AGENT_ID,
          PublishedListingFields.USER_TYPE,
          PublishedListingFields.PUBLISHER_ID,
          PublishedListingFields.CID
        ),
        Map(
          "" -> "unknown"
        )
      )
      .na
      .fill("unknown")

  }

  private def buildPreviousA0Df(config: Config): Option[DataFrame] = {
    val reportTime = config.getDailyReportTime
    getPreviousA0DfFromS3(config.getJobId, reportTime, config)

  }

  private def buildFirstPublishDf(dailyDf: DataFrame, previousA0Df: Option[DataFrame]): DataFrame = {
    val df = dailyDf.dropDuplicateCols(Seq(PublishedListingFields.PROPERTY_ID), col(PublishedListingFields.TIMESTAMP))
    val firstPublishedDf = previousA0Df match {
      case None => df
      case Some(previousA0Df) =>
        df.join(
          previousA0Df.select(col(PublishedListingFields.LISTING_CHANNEL), col(PublishedListingFields.PROPERTY_ID)),
          df(PublishedListingFields.LISTING_CHANNEL) === previousA0Df(PublishedListingFields.LISTING_CHANNEL) &&
            df(PublishedListingFields.PROPERTY_ID) === previousA0Df(PublishedListingFields.PROPERTY_ID),
          "leftanti"
        )
    }

    firstPublishedDf.withColumn(PublishedListingFields.PUBLISH_TYPE, lit(PublishTypes.FIRST_PUBLISHED))
  }

  private def buildRePublishNewOwnerDf(nonFirstPublishedDf: DataFrame, previousA0Df: Option[DataFrame]): DataFrame = {
    val df = previousA0Df match {
      case None => SparkSession.active.emptyDataset(RowEncoder(nonFirstPublishedDf.schema))
      case Some(previousA0Df) =>
        nonFirstPublishedDf
          .dropDuplicateCols(
            Seq(PublishedListingFields.PROPERTY_ID, PublishedListingFields.CID),
            col(PublishedListingFields.TIMESTAMP)
          )
          .joinWith(
            previousA0Df,
            nonFirstPublishedDf(PublishedListingFields.LISTING_CHANNEL) === previousA0Df(
              PublishedListingFields.LISTING_CHANNEL
            ) &&
              nonFirstPublishedDf(PublishedListingFields.PROPERTY_ID) === previousA0Df(
                PublishedListingFields.PROPERTY_ID
              ),
            "inner"
          )
          .select(
            col("_1.*"),
            col(s"_2.${PublishedListingFields.PUBLISHED_INFOS}").as(PublishedListingFields.PUBLISHED_INFOS)
          )
          .withColumn("last_publish_info", element_at(col(PublishedListingFields.PUBLISHED_INFOS), -1)) // last element
          .withColumn("last_publish_owner", col("last_publish_info.cid"))
          .where(col(PublishedListingFields.CID) =!= col("last_publish_owner"))
          .drop("last_publish_info", "last_publish_owner")
    }

    df.withColumn(PublishedListingFields.PUBLISH_TYPE, lit(PublishTypes.REPUBLISHED_NEW_OWNER))

  }

  private def buildRePublishAfter30DayDf(
      nonFirstPublishedDf: DataFrame,
      republishedNewOwnerDf: DataFrame,
      previousA0Df: Option[DataFrame],
      config: Config
  ): DataFrame = {

    val republishedGapDuration = config.getLong("republish_gap_duration", 30.days.toMillis)

    val resultDf = previousA0Df match {
      case None => SparkSession.active.emptyDataset(RowEncoder(nonFirstPublishedDf.schema))
      case Some(previousA0Df) =>
        val df = nonFirstPublishedDf.join(
          republishedNewOwnerDf,
          nonFirstPublishedDf(PublishedListingFields.PROPERTY_ID) === republishedNewOwnerDf(
            PublishedListingFields.PROPERTY_ID
          ),
          "leftanti"
        )

        df.dropDuplicateCols(Seq(PublishedListingFields.PROPERTY_ID), col(PublishedListingFields.TIMESTAMP))
          .joinWith(
            previousA0Df,
            df(PublishedListingFields.LISTING_CHANNEL) === previousA0Df(PublishedListingFields.LISTING_CHANNEL) &&
              df(PublishedListingFields.PROPERTY_ID) === previousA0Df(PublishedListingFields.PROPERTY_ID),
            "inner"
          )
          .select(
            col("_1.*"),
            col(s"_2.${PublishedListingFields.PUBLISHED_INFOS}").as(PublishedListingFields.PUBLISHED_INFOS)
          )
          .withColumn("last_publish_info", element_at(col(PublishedListingFields.PUBLISHED_INFOS), -1))
          .withColumn(
            "last_publish_time_gap",
            col(PublishedListingFields.TIMESTAMP) - col("last_publish_info.timestamp")
          )
          .where(col("last_publish_time_gap") >= lit(republishedGapDuration))
          .drop("last_publish_info", "last_publish_time_gap")
    }

    resultDf.withColumn(PublishedListingFields.PUBLISH_TYPE, lit(PublishTypes.REPUBLISHED_AFTER_30DAYS))

  }

  /** Return a new dataframe with 3 columns: <br>
    * + Channel <br>
    * + Listing Id <br>
    * + Publish Times: cid & time <br>
    *
    * @param previousA0Df
    * @param dailyDf
    * @return
    */
  private def buildA0Df(previousA0Df: Option[DataFrame], dailyDf: DataFrame): DataFrame = {

    val schema = ArrayType(
      StructType(
        Seq(
          StructField(PublishedListingFields.CID, DataTypes.StringType, false),
          StructField(PublishedListingFields.TIMESTAMP, DataTypes.LongType, false)
        )
      )
    )
    val flattenPublishedInfoUdf = udf(
      (publishInfos: Seq[Row]) => {

        val sortedByTimeItems = publishInfos
          .map { row =>
            val cid = row.getAs[String](PublishedListingFields.CID)
            val timestamp = row.getAs[Long](PublishedListingFields.TIMESTAMP)

            (cid, timestamp)
          }
          .distinct
          .sortBy(_._2)

        sortedByTimeItems.map(e => Row(e._1, e._2))
      } ,
      schema
    )

    val df = dailyDf
      .groupBy(
        col(PublishedListingFields.LISTING_CHANNEL),
        col(PublishedListingFields.PROPERTY_ID)
      )
      .agg(
        collect_list(
          struct(
            col(PublishedListingFields.CID),
            col(PublishedListingFields.TIMESTAMP)
          )
        ).as(PublishedListingFields.PUBLISHED_INFOS)
      )

    val a0Df = previousA0Df match {
      case None => df
      case Some(previousA0Df) =>
        previousA0Df
          .unionByName(df, allowMissingColumns = true)
          .groupBy(
            col(PublishedListingFields.LISTING_CHANNEL),
            col(PublishedListingFields.PROPERTY_ID)
          )
          .agg(
            collect_list(col(PublishedListingFields.PUBLISHED_INFOS)).as(PublishedListingFields.PUBLISHED_INFOS)
          )
          .withColumn(PublishedListingFields.PUBLISHED_INFOS, flatten(col(PublishedListingFields.PUBLISHED_INFOS)))
    }

    a0Df.withColumn(
      PublishedListingFields.PUBLISHED_INFOS,
      flattenPublishedInfoUdf(col(PublishedListingFields.PUBLISHED_INFOS))
    )
  }


  def writePublishedListingDataMartToCH(df: Dataset[Row], config: Config): Unit = {
    def toPublishedListingDataMartRecord(row: Row): JsonNode = {
      val recordMap = Map(
        PublishedListingFields.DATE -> row.getAs[Long](PublishedListingFields.DATE),
        PublishedListingFields.LISTING_CHANNEL -> row.getAs[String](PublishedListingFields.LISTING_CHANNEL),
        PublishedListingFields.PUBLISH_TYPE -> row.getAs[String](PublishedListingFields.PUBLISH_TYPE),
        PublishedListingFields.SERVICE_TYPE -> row.getAs[String](PublishedListingFields.SERVICE_TYPE),
        PublishedListingFields.BUSINESS_UNIT -> row.getAs[String](PublishedListingFields.BUSINESS_UNIT),
        PublishedListingFields.MARKET_CENTER_ID -> row.getAs[String](PublishedListingFields.MARKET_CENTER_ID),
        PublishedListingFields.TEAM_ID -> row.getAs[String](PublishedListingFields.TEAM_ID),
        PublishedListingFields.CITY_ID -> row.getAs[String](PublishedListingFields.CITY_ID),
        PublishedListingFields.DISTRICT_ID -> row.getAs[String](PublishedListingFields.DISTRICT_ID),
        PublishedListingFields.PROJECT_ID -> row.getAs[String](PublishedListingFields.PROJECT_ID),
        PublishedListingFields.AGENT_ID -> row.getAs[String](PublishedListingFields.AGENT_ID),
        PublishedListingFields.USER_TYPE -> row.getAs[String](PublishedListingFields.USER_TYPE),
        PublishedListingFields.PUBLISHER_ID -> row.getAs[String](PublishedListingFields.PUBLISHER_ID),
        PublishedListingFields.CID -> row.getAs[String](PublishedListingFields.CID),
        PublishedListingFields.PROPERTY_ID -> row.getAs[String](PublishedListingFields.PROPERTY_ID)
      )
      JsonUtils.toJsonNode(JsonUtils.toJson(recordMap))
    }

    val topic = config.get("published_listing_dataset_topic")
    ingestDataframe(config, df, topic)(toPublishedListingDataMartRecord)
    mergeIfRequired(config,df,topic)
  }
}
