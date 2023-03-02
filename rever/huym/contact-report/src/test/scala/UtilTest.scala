import rever.etl.rsparkflow.domain.IntervalTypes
import rever.etl.rsparkflow.utils.TimestampUtils

/** @author anhlt (andy)
  * @since 27/04/2022
  */
object UtilTest {
  def main(args: Array[String]): Unit = {

    val time = TimestampUtils.parseMillsFromString("2022-06-01", "yyyy-MM-dd")

    val (start, end) = TimestampUtils.getFromAndToDate(time, IntervalTypes.WEEKLY)

    println(TimestampUtils.format(start))
    println(TimestampUtils.format(end))

  }
}
