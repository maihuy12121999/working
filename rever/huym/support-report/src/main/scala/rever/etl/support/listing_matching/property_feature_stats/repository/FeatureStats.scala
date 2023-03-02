package rever.etl.support.listing_matching.property_feature_stats.repository

import vn.rever.common.domain.Implicits.ImplicitAny
import vn.rever.jdbc.JdbcRecord

object FeatureStats {
  final val FIELD_NAME = "field"
  final val MIN = "min_value"
  final val MAX = "max_value"
  final val MEAN = "mean_value"
  final val UNIQUE_VALUES = "unique_values"
  final val MODEL_VERSION = "model_version"
  final val LOG_TIME = "log_time"

  val PRIMARY_KEYS = Seq(FIELD_NAME)

  val FIELDS = Seq(
    FIELD_NAME,
    MIN,
    MAX,
    MEAN,
    UNIQUE_VALUES,
    MODEL_VERSION,
    LOG_TIME
  )

  def empty: FeatureStats = {
    FeatureStats(
      fieldName = None,
      min = None,
      max = None,
      mean = None,
      uniqueValues = None,
      modelVersion = None,
      logTime = None
    )
  }
}

case class FeatureStats(
    var fieldName: Option[String],
    var min: Option[Double],
    var max: Option[Double],
    var mean: Option[Double],
    var uniqueValues: Option[Seq[String]],
    var modelVersion: Option[Int],
    var logTime: Option[Long]
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = FeatureStats.PRIMARY_KEYS

  override def getFields(): Seq[String] = FeatureStats.FIELDS

  override def setValues(field: String, value: Any): Unit = {
    field match {
      case FeatureStats.FIELD_NAME    => fieldName = value.asOpt
      case FeatureStats.MIN           => min = value.asOpt
      case FeatureStats.MAX           => max = value.asOpt
      case FeatureStats.MEAN          => mean = value.asOpt
      case FeatureStats.UNIQUE_VALUES =>
      case FeatureStats.MODEL_VERSION => modelVersion = value.asOpt
      case FeatureStats.LOG_TIME      => logTime = value.asOpt
      case _                          =>
    }
  }

  override def getValue(field: String): Option[Any] = {
    field match {
      case FeatureStats.FIELD_NAME    => fieldName
      case FeatureStats.MIN           => min
      case FeatureStats.MAX           => max
      case FeatureStats.MEAN          => mean
      case FeatureStats.UNIQUE_VALUES => uniqueValues.map(_.toArray)
      case FeatureStats.MODEL_VERSION => modelVersion
      case FeatureStats.LOG_TIME      => logTime

    }
  }
}
