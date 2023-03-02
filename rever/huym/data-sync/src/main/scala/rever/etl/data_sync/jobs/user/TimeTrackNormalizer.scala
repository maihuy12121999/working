package rever.etl.data_sync.jobs.user

import rever.etl.data_sync.core.Normalizer
import rever.etl.data_sync.domain.GenericRecord
import rever.etl.data_sync.domain.user_activity.{TimeTrack, TimeTrackDM}

/** @author anhlt (andy)
  */
case class TimeTrackNormalizer() extends Normalizer[GenericRecord, Map[String, Any]] {

  override def toRecord(record: GenericRecord, total: Long): Option[Map[String, Any]] = {

    val user = TimeTrack.fromGenericRecord(record)

    val dataMap = Map[String, Any](
      TimeTrackDM.ID -> user.id.get,
      TimeTrackDM.USERNAME -> user.username.getOrElse(""),
      TimeTrackDM.WORK_EMAIL -> user.workEmail.getOrElse(""),
      TimeTrackDM.TIME -> user.time.getOrElse(0L),
      TimeTrackDM.TIME_STRING -> user.timeString.getOrElse(""),
      TimeTrackDM.SCHEDULER_ID -> user.schedulerId.getOrElse(0),
      TimeTrackDM.MARKET_CENTER_ID -> user.marketCenterId.getOrElse(""),
      TimeTrackDM.TEAM_ID -> user.teamId.getOrElse(""),
      TimeTrackDM.WORK_DURATION -> user.workDuration.getOrElse(0),
      TimeTrackDM.FIRST_IN_TIME -> user.firstInTime.getOrElse(0L),
      TimeTrackDM.LAST_OUT_TIME -> user.lastOutTime.getOrElse(0L),
      TimeTrackDM.LATEST_CLOCK_TIME -> user.latestClockTime.getOrElse(0L),
      TimeTrackDM.CLOCKS -> user.clocks.getOrElse(Seq.empty).map(_.toString).toArray,
      TimeTrackDM.CLOCK_IN -> user.getClockIn.map(_.toString).getOrElse("{}"),
      TimeTrackDM.CLOCK_OUT -> user.getClockOut.map(_.toString).getOrElse("{}"),
      TimeTrackDM.STATUS -> user.getTimeTrackStatusName(),
      TimeTrackDM.CREATED_TIME -> user.createdTime.getOrElse(0L),
      TimeTrackDM.CREATED_BY -> user.createdBy.getOrElse(""),
      TimeTrackDM.UPDATED_TIME -> user.updatedTime.getOrElse(0L),
      TimeTrackDM.UPDATED_BY -> user.updatedBy.getOrElse(""),
      TimeTrackDM.LOG_TIME -> System.currentTimeMillis()
    )
    Some(dataMap)
  }

}
