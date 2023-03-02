package rever.etl.support.listing_matching.property_feature_stats.repository

import rever.rsparkflow.dao.clickhouse.CustomClickhouseDAO

import javax.sql.DataSource

case class FeatureStatsDAO(ds: DataSource, table: String) extends CustomClickhouseDAO[FeatureStats] {
  override def createRecord(): FeatureStats = FeatureStats.empty
}
