package rever.etl.data_sync.util

import org.apache.spark.sql.functions.udf
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import rever.etl.rsparkflow.domain.ExecutionDateInfo
import rever.etl.rsparkflow.utils.TimestampUtils

/** @author anhlt (andy)
  * @since 09/07/2022
  */
object Utils {
  final val normalizedDateUdf = udf[Long, String](normalizedConvertDateToTimestamp)

  def buildSyncDataESQueryByTimeRange(
      config: ExecutionDateInfo,
      timeField: String,
      isSyncAll: Boolean = false
  ): QueryBuilder = {

    if (isSyncAll) {
      QueryBuilders
        .boolQuery()
        .must(QueryBuilders.matchAllQuery())
    } else {
      val (fromTime, toTime) = config.getSyncDataTimeRange
      QueryBuilders
        .boolQuery()
        .must(QueryBuilders.rangeQuery(timeField).gte(fromTime).lt(toTime))
    }

  }

  implicit class ImplicitAny(val value: Any) extends AnyVal {
    def asOpt[T]: Option[T] = value.asOptAny.map(_.asInstanceOf[T])

    def asOptInt: Option[Int] = value.asOptAny.map(_.asInstanceOf[Int])

    def asOptLong: Option[Long] = value.asOptAny.map(_.asInstanceOf[Long])

    def asOptString: Option[String] = value.asOptAny.map(_.asInstanceOf[String])

    def asOptDouble: Option[Double] = value.asOptAny.map(_.asInstanceOf[Double])

    def asOptFloat: Option[Float] = value.asOptAny.map(_.asInstanceOf[Float])

    def asOptBoolean: Option[Boolean] = value.asOptAny.map(_.asInstanceOf[Boolean])

    def asOptShort: Option[Short] = value.asOptAny.map(_.asInstanceOf[Short])

    def asOptAny: Option[Any] = value match {
      case s: Option[_] => s
      case _            => Option(value)
    }

    def orEmpty: String = value match {
      case Some(x) => x.toString
      case _       => value.toString
    }
  }

  def normalizedConvertDateToTimestamp(date: String): Long = {
    date match {
      case null | "unknown" | "" => 0L
      case _ =>
        Seq(
          scala.util.Try(TimestampUtils.parseMillsFromString(date.trim, "dd-MMM-yy")).toOption,
          scala.util.Try(TimestampUtils.parseMillsFromString(date.trim, "dd-MMM-yyyy")).toOption
        ).flatten.toSeq.headOption.getOrElse(0L)
    }
  }

  def computeGender(gender: Option[String], personalTitle: Option[String]): Option[String] = {
    Seq(
      gender
      //      personalTitle.map(interpolateSexFromPersonalTitle)
    ).flatten
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .headOption
  }

  def interpolateSexFromPersonalTitle(personalTitle: String): String = {
    personalTitle match {
      case "anh" | "chu" => "male"
      case "chi" | "co"  => "female"
      case _             => ""
    }
  }

}
