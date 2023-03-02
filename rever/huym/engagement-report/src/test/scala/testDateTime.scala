import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.api.udf.RUdfUtils
import rever.etl.rsparkflow.utils.{TimestampUtils, Utils}

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.{Calendar, Date}
import scala.collection.JavaConverters.mapAsJavaMapConverter

object testDateTime {
  def main(args: Array[String]): Unit = {
    val config = Config
      .builder()
      .addArgument("CH_DRIVER", true)
      .addArgument("CH_HOST", true)
      .addArgument("CH_PORT", true)
      .addArgument("CH_USER_NAME", true)
      .addArgument("CH_PASSWORD", true)
      .addArgument("RV_JOB_ID", true)
      .addArgument("RV_EXECUTION_DATE", true)
      .addArgument("RV_DATA_MAPPING_HOST", true)
      .addArgument("RV_DATA_MAPPING_USER", true)
      .addArgument("RV_DATA_MAPPING_PASSWORD", true)
      .addArgument("RV_RAP_INGESTION_HOST", true)
      .addArgument("RV_S3_ACCESS_KEY", true)
      .addArgument("RV_S3_SECRET_KEY", true)
      .addArgument("RV_S3_REGION", true)
      .addArgument("RV_S3_BUCKET", true)
      .addArgument("RV_S3_PARENT_PATH", true)
      .build(Utils.parseArgToMap(args).asJava, true)

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()
    SparkSession.active.sparkContext.setLogLevel("ERROR")
    val reportTime = config.getDailyReportTime
    println(reportTime)
    val dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy")
    val reportDay = new SimpleDateFormat("dd/MM/yyyy").format(reportTime)
    val dayOfWeek = LocalDate.parse(reportDay,dateTimeFormatter).getDayOfWeek
    println(dayOfWeek)
    val sdf = new SimpleDateFormat("dd-M-yyyy hh:mm:ss")
    val reportDayTimeInString = new SimpleDateFormat("dd-M-yyyy hh:mm:ss").format(reportTime)
    val dateTimeReportTime = sdf.parse(reportDayTimeInString)
    println(dateTimeReportTime)
    val calendarReportTime = dateToCalendar(dateTimeReportTime)
    val getStartOfWeekIn = getStartOfWeekInTimeStamp(calendarReportTime)
    println(calendarReportTime.getTime)
    println(TimestampUtils.asStartOfWeek(reportTime))
  }
  def dateToCalendar(date:Date): Calendar ={
    val calendar:Calendar = Calendar.getInstance()
    calendar.clear(Calendar.MINUTE)
    calendar.clear(Calendar.SECOND)
    calendar.clear(Calendar.MILLISECOND)
    calendar
  }
  def getStartOfWeekInTimeStamp(calendar: Calendar):Calendar={


    calendar.set(Calendar.DAY_OF_WEEK,calendar.getFirstDayOfWeek+1)
    calendar
  }
}
