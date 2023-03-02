package rever.etl.support.listing_matching.property_feature_stats.writer

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import rever.etl.support.listing_matching.property_feature_stats.FeatureStatsHelper
import rever.etl.support.listing_matching.property_feature_stats.repository.{FeatureStats, FeatureStatsDAO}
import rever.rsparkflow.dao.DAOFactory
import rever.rsparkflow.implicits.ConfigImplicits.ConfigImplicit
import rever.rsparkflow.spark.api.SinkWriter
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.extensions.RapIngestWriterMixin

import scala.collection.JavaConverters.asScalaBufferConverter

class FeatureStatsWriter extends SinkWriter with RapIngestWriterMixin {

  override def write(s: String, df: Dataset[Row], config: Config): Dataset[Row] = {
    val catFeatures = config.getList("categorical_features", ",").asScala.distinct
    val numFeatures = config.getList("numerical_features", ",").asScala.distinct
    val savedFeatureType = config.get("saved_feature_type")

    val resultDf = savedFeatureType match {
      case "all"         => df
      case "categorical" => df.where(col(FeatureStatsHelper.FIELD).isin(catFeatures: _*))
      case "numerical"   => df.where(col(FeatureStatsHelper.FIELD).isin(numFeatures: _*))
      case _             => df
    }

    ingestDataframeToCH(config, resultDf)
    resultDf
  }

  private def ingestDataframeToCH(config: Config, df: DataFrame): Unit = {
    val insertedCounter = SparkSession.active.sparkContext.longAccumulator("inserted_counter")

    val table = config.get("CH_TABLE")
    val modelVersion = config.getInt("model_version", 1)
    val mergeAfterWrite = config.getBoolean("merge_after_write", false)

    df.rdd.foreachPartition(rows => {
      val featureStatsDAO = DAOFactory.singletonClickhouse(config.clickHouseConfig)(FeatureStatsDAO(_, table))
      rows.toSeq
        .grouped(500)
        .map(toFeatureStats(_, modelVersion))
        .foreach(records => {
          val totalInsertedCount = featureStatsDAO.multiInsert(records)
          insertedCounter.add(totalInsertedCount)
        })
    })

    if (mergeAfterWrite && !insertedCounter.isZero) {
      val featureStatsDAO = DAOFactory.singletonClickhouse(config.clickHouseConfig)(FeatureStatsDAO(_, table))
      featureStatsDAO.optimizeTable(false)
    }

    println(s"Inserted: ${insertedCounter.value} records to Listing Matching feature stats")
  }

  private def toFeatureStats(rows: Seq[Row], modelVersion: Int): Seq[FeatureStats] = {
    rows.map(row => {
      val field = row.getAs[String](FeatureStatsHelper.FIELD)
      val minValue = row.getAs[Double](FeatureStatsHelper.MIN_VALUE)
      val maxValue = row.getAs[Double](FeatureStatsHelper.MAX_VALUE)
      val meanValue = row.getAs[Double](FeatureStatsHelper.MEAN_VALUE)
      val uniqueValues = row.getAs[Seq[String]](FeatureStatsHelper.UNIQUE_VALUES).toArray

      FeatureStats(
        fieldName = Some(field),
        min = Some(minValue),
        max = Some(maxValue),
        mean = Some(meanValue),
        uniqueValues = Some(uniqueValues),
        modelVersion = Some(modelVersion),
        logTime = Some(System.currentTimeMillis())
      )
    })
  }
}
