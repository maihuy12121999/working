package rever.etl.data_sync.jobs.daily_checkin

import rever.etl.data_sync.core.Normalizer
import rever.etl.data_sync.domain.GenericRecord
import rever.etl.data_sync.domain.user_activity.{DailyCheckin, DailyCheckinDM}

/** @author anhlt (andy)
  */
case class DailyCheckinNormalizer() extends Normalizer[GenericRecord, Map[String, Any]] {

  override def toRecord(record: GenericRecord, total: Long): Option[Map[String, Any]] = {

    val dailyCheckin = DailyCheckin.fromGenericRecord(record)

    val dataMap = Map[String, Any](
      DailyCheckinDM.DATE_STRING -> dailyCheckin.dateString.get,
      DailyCheckinDM.USERNAME -> dailyCheckin.username.getOrElse(""),
      DailyCheckinDM.TIMESTAMP -> dailyCheckin.timestamp.getOrElse(0L),
      DailyCheckinDM.MARKET_CENTER -> dailyCheckin.marketCenterId.getOrElse(""),
      DailyCheckinDM.TEAM -> dailyCheckin.teamId.getOrElse(""),
      DailyCheckinDM.FIRST_ANSWER -> dailyCheckin.firstAnswer.getOrElse(""),
      DailyCheckinDM.FIRST_ANSWER_AT -> dailyCheckin.firstAnswerAt.getOrElse(0L),
      DailyCheckinDM.SECOND_ANSWER -> dailyCheckin.secondAnswer.getOrElse(""),
      DailyCheckinDM.SECOND_ANSWER_AT -> dailyCheckin.secondAnswerAt.getOrElse(0L),
      DailyCheckinDM.THIRD_ANSWER -> dailyCheckin.thirdAnswer.getOrElse(""),
      DailyCheckinDM.THIRD_ANSWER_AT -> dailyCheckin.thirdAnswerAt.getOrElse(0L),
      DailyCheckinDM.CHECKIN_STATUS -> DailyCheckinDM.toCheckinStatusName(dailyCheckin.checkinStatus.get),
      DailyCheckinDM.CREATED_TIME -> dailyCheckin.createdTime.getOrElse(0L),
      DailyCheckinDM.UPDATED_TIME -> dailyCheckin.updatedTime.getOrElse(0L),
      "log_time" -> System.currentTimeMillis()
    )
    Some(dataMap)
  }

}
