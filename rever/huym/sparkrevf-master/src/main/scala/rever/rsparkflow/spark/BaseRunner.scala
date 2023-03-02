package rever.rsparkflow.spark

import org.apache.spark.sql.SparkSession
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.utils.TimestampUtils

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.concurrent.duration.DurationInt

/**
  * @author anhlt (andy)
  * @since 02/07/2022
**/
trait BaseRunner {

  final protected def runWithDailyForceRun(packageName: String, config: Config): Unit = {

    val isForceRun = config.getBoolean("forcerun.enabled", false)

    if (!isForceRun) {
      new RSparkFlow().run(packageName, config)
    } else {
      val fromDate = config.get("forcerun.from_date")
      val toDate = config.get("forcerun.to_date")
      runDailyWithCatchup(
        config,
        packageName,
        startTime = TimestampUtils.parseMillsFromString(fromDate, "yyyy-MM-dd"),
        endTime = TimestampUtils.parseMillsFromString(toDate, "yyyy-MM-dd")
      )
    }

  }

  final protected def runDailyWithCatchup(config: Config, packageName: String, maxCatchupDay: Int = 31): Unit = {
    val enableCatchupRun = config.getBoolean("enable_catchup_run", false)
    val isYesterday = TimestampUtils.isSameDay(
      TimestampUtils.asStartOfDay(config.getDailyReportTime),
      TimestampUtils.asStartOfDay(System.currentTimeMillis()) - 1.days.toMillis
    )

    val numOfDay = if (enableCatchupRun && isYesterday) maxCatchupDay else 0

    val startReportTime = config.getDailyReportTime - numOfDay.days.toMillis
    val endReportTime = config.getDailyReportTime

    for (reportTime <- startReportTime to (endReportTime, 1.days.toMillis)) {
      val newConfig = config.cloneWith(
        Map[String, AnyRef](
          "RV_EXECUTION_DATE" -> TimestampUtils.format(reportTime, Some("yyyy-MM-dd'T'HH:mm:ssXXX"))
        ).asJava
      )
      new RSparkFlow().run(packageName, newConfig)
      SparkSession.active.sqlContext.clearCache()
    }
  }

  final protected def runDailyWithCatchup(config: Config, packageName: String, startTime: Long, endTime: Long): Unit = {

    val startReportTime = startTime
    val endReportTime = endTime

    for (reportTime <- startReportTime to (endReportTime, 1.days.toMillis)) {
      val newConfig = config.cloneWith(
        Map[String, AnyRef](
          "RV_EXECUTION_DATE" -> TimestampUtils.format(reportTime, Some("yyyy-MM-dd'T'HH:mm:ssXXX"))
        ).asJava
      )
      new RSparkFlow().run(packageName, newConfig)
      SparkSession.active.sqlContext.clearCache()

      println("----------------------------------------------------------------------")
    }
  }

}
