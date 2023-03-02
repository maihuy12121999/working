package rever.etl.listing.view_impressions

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import rever.etl.listing.view_impressions.reader.ViewListingEventReader
import rever.etl.listing.view_impressions.writer.ListingImpressionWriter
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.api.udf.RUdfUtils

import scala.concurrent.duration.DurationInt

class ListingImpression extends FlowMixin {
  @Output(writer = classOf[ListingImpressionWriter])
  @Table("listing_impression")
  def build(
      @Table(
        name = "segment_tracking.raw_data_normalized",
        reader = classOf[ViewListingEventReader]
      ) eventDf: DataFrame,
      config: Config
  ): DataFrame = {
    val dailyDf = eventDf
      .withColumn(
        "domain",
        callUDF(RUdfUtils.RV_GET_DOMAIN, col("url"), lit("unknown"))
      )
      .groupBy(
        col("domain"),
        col("property_id")
      )
      .agg(
        countDistinct("anonymous_id").as("total_viewers"),
        countDistinct("user_id").as("total_users"),
        count("property_id").as("total_action")
      )
      .persist(StorageLevel.MEMORY_AND_DISK_2)

    val a0Df = buildA0Df(dailyDf.drop("total_viewers", "total_users"), config)
    exportA1DfToS3(config.getJobId, config.getDailyReportTime, dailyDf, config)
    exportA0DfToS3(config.getJobId, config.getDailyReportTime, a0Df, config)

    val a1ResultDf = dailyDf.withColumnRenamed("total_action", "a1")
    val a7ResultDf = buildA7ResultDf(config)
    val a30ResultDf = buildA30ResultDf(config)

    a0Df
      .withColumnRenamed("total_action", "a0")
      .join(a1ResultDf, Seq("domain", "property_id"), "outer")
      .join(a7ResultDf, Seq("domain", "property_id"), "outer")
      .join(a30ResultDf, Seq("domain", "property_id"), "outer")
      .groupBy("domain", "property_id")
      .agg(
        sum("total_viewers").as("total_a1_viewers"),
        sum("total_users").as("total_a1_users"),
        sum("a1").as("a1"),
        sum("a7").as("a7"),
        sum("a30").as("a30"),
        sum("a0").as("a0")
      )
      .withColumn(
        "total_a1_viewers",
        when(col("total_a1_viewers").isNull, lit(0)).otherwise(col("total_a1_viewers"))
      )
      .withColumn(
        "total_a1_users",
        when(col("total_a1_users").isNull, lit(0)).otherwise(col("total_a1_users"))
      )
      .withColumn("a1", when(col("a1").isNull, lit(0)).otherwise(col("a1")))
      .withColumn("a7", when(col("a7").isNull, lit(0)).otherwise(col("a7")))
      .withColumn("a30", when(col("a30").isNull, lit(0)).otherwise(col("a30")))
      .withColumn("a0", when(col("a0").isNull, lit(0)).otherwise(col("a0")))
  }

  private def buildA0Df(dailyDf: DataFrame, config: Config): DataFrame = {
    val reportTime = config.getDailyReportTime
    getPreviousA0DfFromS3(config.getJobId, reportTime, config) match {
      case Some(previousA0Df) =>
        previousA0Df
          .unionByName(dailyDf, true)
          .groupBy(
            col("domain"),
            col("property_id")
          )
          .agg(sum(col("total_action")).as("total_action"))
          .persist(StorageLevel.MEMORY_AND_DISK_2)
      case None => dailyDf
    }
  }

  private def buildA7ResultDf(config: Config): DataFrame = {
    buildResultDf(config, config.getDailyReportTime - 6.days.toMillis, config.getDailyReportTime)
      .withColumnRenamed("total_action", "a7")
      .persist(StorageLevel.MEMORY_AND_DISK_2)

  }

  private def buildA30ResultDf(config: Config): DataFrame = {

    buildResultDf(config, config.getDailyReportTime - 29.days.toMillis, config.getDailyReportTime)
      .withColumnRenamed("total_action", "a30")
      .persist(StorageLevel.MEMORY_AND_DISK_2)

  }

  private def buildResultDf(config: Config, fromTime: Long, toTime: Long): DataFrame = {
    loadAllA1DfFromS3(config, config.getJobId, fromTime, toTime) match {
      case Some(df) =>
        df.groupBy(
          col("domain"),
          col("property_id")
        ).agg(
          sum("total_action").as("total_action")
        )
      case None => SparkSession.active.emptyDataset(RowEncoder(ListingHelper.finalSchema))
    }
  }

}
