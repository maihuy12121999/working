package rever.etl.data_sync.domain.standardized_oppo_historical

import vn.rever.common.domain.Implicits.ImplicitAny
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}
object OppoHistoricalRecord {
  final val OBJECT_ID = "object_id"
  final val OLD_DATA = "old_data"
  final val DATA = "data"
  final val TIMESTAMP = "timestamp"
  final val ACTION = "action"
  final val PERFORMER = "performer"
  final val SOURCE = "source"
  final val LOG_TIME = "log_time"
  final val ID = "id"

  final val PRIMARY_IDS = Seq(TIMESTAMP, OBJECT_ID)
  final val FIELDS = Seq(
    TIMESTAMP,
    OLD_DATA,
    DATA,
    PERFORMER,
    SOURCE,
    ACTION,
    OBJECT_ID
  )

}

case class OppoHistoricalRecord(
                                 var timestamp: Option[Long] = None,
                                 var oldData: Option[String] = None,
                                 var newData: Option[String] = None,
                                 var performer: Option[String] = None,
                                 var source: Option[String] = None,
                                 var action: Option[String] = None,
                                 var oppoId: Option[String] = None,
                                 var logTime: Option[String] = None,
                                 var id: Option[String] = None
                               ) extends JdbcRecord {
  override def getPrimaryKeys(): Seq[String] = OppoHistoricalRecord.PRIMARY_IDS

  override def getFields(): Seq[String] = OppoHistoricalRecord.FIELDS

  override def setValues(field: String, value: Any): Unit = field match {
    case OppoHistoricalRecord.LOG_TIME   => logTime = value.asOpt
    case OppoHistoricalRecord.ID   => id = value.asOpt
    case OppoHistoricalRecord.TIMESTAMP => timestamp = value.asOpt
    case OppoHistoricalRecord.OLD_DATA  => oldData = value.asOpt
    case OppoHistoricalRecord.DATA  => newData = value.asOpt
    case OppoHistoricalRecord.PERFORMER => performer = value.asOpt
    case OppoHistoricalRecord.SOURCE    => source = value.asOpt
    case OppoHistoricalRecord.ACTION    => action = value.asOpt
    case OppoHistoricalRecord.OBJECT_ID   => oppoId = value.asOpt
    case _         =>
  }

  override def getValue(field: String): Option[Any] = field match {
    case OppoHistoricalRecord.LOG_TIME   => logTime
    case OppoHistoricalRecord.ID   => id
    case OppoHistoricalRecord.TIMESTAMP => timestamp
    case OppoHistoricalRecord.OLD_DATA  => oldData
    case OppoHistoricalRecord.DATA  => newData
    case OppoHistoricalRecord.PERFORMER => performer
    case OppoHistoricalRecord.SOURCE    => source
    case OppoHistoricalRecord.ACTION    => action
    case OppoHistoricalRecord.OBJECT_ID   => oppoId
    case _         => throw SqlFieldMissing(field)
  }

  def toMap: Map[String, Any] = {
    OppoHistoricalRecord.FIELDS
      .map(field => field -> getValue(field).get)
      .toMap
  }
}
