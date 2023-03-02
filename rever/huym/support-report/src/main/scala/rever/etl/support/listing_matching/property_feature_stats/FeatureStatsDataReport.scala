package rever.etl.support.listing_matching.property_feature_stats

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import rever.rsparkflow.spark.FlowMixin
import rever.rsparkflow.spark.api.annotation.{Output, Table}
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.utils.JsonUtils
import rever.etl.support.listing_matching.property_feature_stats.FeatureStatsHelper.ListingMatchingStatsConfig
import rever.etl.support.listing_matching.property_feature_stats.reader.ListingDMReader
import rever.etl.support.listing_matching.property_feature_stats.writer.FeatureStatsWriter

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps

object FeatureStatsDataReport {
  def buildNormalizeNumericalUdf(config: ListingMatchingStatsConfig): UserDefinedFunction = {
    udf((fieldName: String, value: Double) => {
      val minValue = config.numericalValidation.get(fieldName).flatMap(_.from)
      val maxValue = config.numericalValidation.get(fieldName).flatMap(_.to)

      (minValue, maxValue) match {
        case (None, None)      => value
        case (None, Some(max)) => math.min(max, value)
        case (Some(min), None) => math.max(min, value)
        case (Some(min), Some(max)) =>
          if (value < min) min
          else math.min(max, value)
      }

    })
  }
}
class FeatureStatsDataReport extends FlowMixin {

  @Output(writer = classOf[FeatureStatsWriter])
  @Table("feature_stats")
  def build(
      @Table(name = "listing_df", reader = classOf[ListingDMReader]) listingDf: DataFrame,
      config: Config
  ): DataFrame = {
    val numericalFeatureDf = computeNumericalFeatures(listingDf, config)
    val categoricalFeatureDf = computeCategoricalFeatures(listingDf, config)
    categoricalFeatureDf.unionByName(numericalFeatureDf, allowMissingColumns = true)
  }

  private def computeCategoricalFeatures(df: DataFrame, config: Config): DataFrame = {
    val catFeatures = config.getList("categorical_features", ",").asScala.distinct
    df.na
      .fill("")
      .flatMap(row => {
        val resultList = ListBuffer.empty[Row]
        catFeatures.foreach(fieldName => resultList.append(Row(fieldName, row.getAs[String](fieldName))))
        resultList
      })(RowEncoder(FeatureStatsHelper.flattenedCatDfSchema))
      .where(
        col("value").isNotNull &&
          not(col("value").isin("", "unknown"))
      )
      .groupBy(FeatureStatsHelper.FIELD)
      .agg(
        lit(0.0).as(FeatureStatsHelper.MIN_VALUE),
        lit(0.0).as(FeatureStatsHelper.MAX_VALUE),
        lit(0.0).as(FeatureStatsHelper.MEAN_VALUE),
        array_sort(collect_set("value")).as(FeatureStatsHelper.UNIQUE_VALUES)
      )
  }

  private def computeNumericalFeatures(df: DataFrame, config: Config): DataFrame = {
    val numFeatures = config.getList("numerical_features", ",").asScala.distinct
    val statsConfig = JsonUtils.fromJson[ListingMatchingStatsConfig](
      config.getAndDecodeBase64("listing_matching_stats_config")
    )

    val normalizer = FeatureStatsDataReport.buildNormalizeNumericalUdf(statsConfig)

    df.flatMap(row => {
      numFeatures.map(fieldName => Row(fieldName, row.getAs[Double](fieldName)))
    })(RowEncoder(FeatureStatsHelper.flattenedNumDfSchema))
      .where(
        col(FeatureStatsHelper.FIELD).isin(numFeatures: _*)
          && col("value") >= 0
      )
      .withColumn(
        "value",
        normalizer(col(FeatureStatsHelper.FIELD), col("value"))
      )
      .groupBy(FeatureStatsHelper.FIELD)
      .agg(
        min("value").as(FeatureStatsHelper.MIN_VALUE),
        max("value").as(FeatureStatsHelper.MAX_VALUE),
        avg("value").as(FeatureStatsHelper.MEAN_VALUE)
      )
      .withColumn(FeatureStatsHelper.UNIQUE_VALUES, typedLit(Seq.empty[String]))
  }

}
