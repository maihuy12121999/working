package rever.etl.data_sync.domain.finance

import rever.etl.data_sync.util.Utils.ImplicitAny
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}

object BoTeamSize {

  final val TBL_NAME = "bo_team_size"

  def field(f: String): String = f

  final val PERIOD_TYPE: String = field("period_type")
  final val PERIOD_VALUE: String = field("period_value")
  final val TEAM: String = field("team")
  final val TEAM_SIZE: String = field("team_size")
  final val TIME: String = field("time")
  final val LOG_TIME: String = field("log_time")

  final val primaryKeys = Seq(PERIOD_TYPE, PERIOD_VALUE, TEAM)

  final val fields = Seq(
    PERIOD_TYPE,
    PERIOD_VALUE,
    TEAM,
    TEAM_SIZE,
    TIME,
    LOG_TIME
  )

  def empty: BoTeamSize = {
    BoTeamSize(
      periodType = None,
      periodValue = None,
      team = None,
      teamSize = None,
      time = None,
      logTime = None
    )
  }
}

case class BoTeamSize(
    var periodType: Option[String],
    var periodValue: Option[String],
    var team: Option[String],
    var teamSize: Option[Int],
    var time: Option[Long],
    var logTime: Option[Long]
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = BoTeamSize.primaryKeys

  override def getFields(): Seq[String] = BoTeamSize.fields

  override def setValues(field: String, value: Any): Unit =
    field match {
      case BoTeamSize.PERIOD_TYPE  => periodType = value.asOpt
      case BoTeamSize.PERIOD_VALUE => periodValue = value.asOpt
      case BoTeamSize.TEAM         => team = value.asOpt
      case BoTeamSize.TEAM_SIZE    => teamSize = value.asOpt
      case BoTeamSize.TIME         => time = value.asOpt
      case BoTeamSize.LOG_TIME     => logTime = value.asOpt
      case _                       => throw SqlFieldMissing(field)
    }

  override def getValue(field: String): Option[Any] = {
    field match {
      case BoTeamSize.PERIOD_TYPE  => periodType
      case BoTeamSize.PERIOD_VALUE => periodValue
      case BoTeamSize.TEAM         => team
      case BoTeamSize.TEAM_SIZE    => teamSize
      case BoTeamSize.TIME         => time
      case BoTeamSize.LOG_TIME     => logTime
      case _                       => throw SqlFieldMissing(field)
    }
  }
}
