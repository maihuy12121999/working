package rever.etl.support.cs2_call_cta_analysis.reader

import org.apache.spark.sql.functions.{callUDF, col, lit, udf}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.rsparkflow.spark.api.SourceReader
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.api.udf.RUdfUtils

import java.net.URL
import java.util.regex.Pattern
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.concurrent.duration.DurationInt

class CallCtaEventReader extends SourceReader {

  override def read(tableName: String, config: Config): Dataset[Row] = {
    val driver = config.get("CH_DRIVER")
    val host = config.get("CH_HOST")
    val port = config.getInt("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val password = config.get("CH_PASSWORD")

    val getAliasFromUrlUdf = udf[String, String](getAliasFromUrl)

    val df = SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port/segment_tracking")
      .options(Map("delimiter" -> ",", "header" -> "true", "inferSchema" -> "true"))
      .option("user", userName)
      .option("password", password)
      .option("driver", driver)
      .option("query", buildQuery(config))
      .load()

    df.withColumn("alias", getAliasFromUrlUdf(col("url")))
      .withColumn("type", lit("cta"))
      .withColumn("ua", callUDF(RUdfUtils.RV_PARSE_USER_AGENT, col("user_agent")))
      .withColumn("ua_os", col("ua.os"))
      .withColumn("ua_platform", col("ua.platform"))
      .drop("user_agent", "ua")
      .orderBy(col("timestamp").asc)

  }

  private def buildQuery(config: Config): String = {
    val reportTime = config.getDailyReportTime

    val callCtaEvents = config.getList("call_cta_events", ",").asScala
    val query = s"""
       |SELECT
       |    message_id,
       |    anonymous_id,
       |    user_id,
       |    JSONExtractString(context,'page','url') as url,
       |    JSONExtractString(context,'userAgent') AS user_agent,
       |    CASE extract(JSONExtractString(context,'page','path'),'/((mua)|(ban)|(du-an)|(chuyen-vien))/.*')
       |      WHEN 'mua' THEN 'listing' 
       |      WHEN 'ban' THEN 'listing' 
       |      WHEN 'du-an' THEN 'project' 
       |      WHEN 'chuyen-vien' THEN 'profile'
       |      ELSE ''
       |    END AS source_type,
       |    event,
       |    toInt64(timestamp) as timestamp
       |FROM raw_data_normalized
       |WHERE
       |    1=1
       |    AND type = 'track'
       |    AND multiMatchAny(url, ['.*rever\\.vn/ban/.*', '.*rever\\.vn/mua/.*', '.*rever\\.vn/du-an/.*', '.*rever\\.vn/chuyen-vien/.*'])
       |    AND event IN (${callCtaEvents.map(s => s"'$s'").mkString(",")})
       |    AND timestamp >= ${reportTime}
       |    AND timestamp < ${reportTime + 1.days.toMillis}
       |""".stripMargin

    query
  }

  private def getAliasFromUrl(url: String): String = {
    try {

      val path = new URL(url).getPath

      val m = Pattern.compile("/((mua)|(ban)|(du-an)|(chuyen-vien))/(?<alias>.+)").matcher(path)
      if (m.find()) {
        m.group("alias")
      } else {
        ""
      }
    } catch {
      case _: Exception => ""
    }
  }
}
