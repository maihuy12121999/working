package rever.etl.data_sync.jobs.user

import rever.etl.data_sync.core.Normalizer
import rever.etl.data_sync.domain.GenericRecord
import rever.etl.data_sync.domain.user_activity.{ClockActionType, ClockInOutRequest}

/** @author anhlt (andy)
  */
case class ClockRequestNormalizer() extends Normalizer[GenericRecord, Map[String, Any]] {

  override def toRecord(record: GenericRecord, total: Long): Option[Map[String, Any]] = {

    val user = ClockInOutRequest.fromGenericRecord(record)

    val dataMap = Map[String, Any](
      ClockInOutRequest.ID -> user.id.get,
      ClockInOutRequest.TYPE -> user.clockType.map(ClockActionType.clockActionName).getOrElse("undefined"),
      ClockInOutRequest.SOURCE -> user.source.getOrElse(""),
      ClockInOutRequest.SOURCE_USER -> user.sourceUser.getOrElse(""),
      ClockInOutRequest.SOURCE_CHANNEL -> user.sourceChannel.getOrElse(""),
      ClockInOutRequest.USERNAME -> user.username.getOrElse(""),
      ClockInOutRequest.WORK_EMAIL -> user.workEmail.getOrElse(""),
      ClockInOutRequest.TIME -> user.time.getOrElse(0L),
      ClockInOutRequest.VERIFIED_TIME -> user.verifiedTime.getOrElse(0L),
      ClockInOutRequest.STATUS -> user.status.getOrElse(0),
      "log_time" -> System.currentTimeMillis()
    )
    Some(dataMap)
  }

}
