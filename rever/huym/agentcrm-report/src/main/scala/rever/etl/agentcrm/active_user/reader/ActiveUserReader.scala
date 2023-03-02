package rever.etl.agentcrm.active_user.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.agentcrm.config.FieldConfig
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.concurrent.duration.DurationInt

class ActiveUserReader extends SourceReader {
  override def read(s: String, config: Config): Dataset[Row] = {
    val driver = config.get("CH_DRIVER")
    val host = config.get("CH_HOST")
    val port = config.getInt("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val password = config.get("CH_PASSWORD")

    val df = SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port/segment_tracking")
      .options(Map("delimiter" -> ",", "header" -> "true", "inferSchema" -> "true"))
      .option("user", userName)
      .option("password", password)
      .option("driver", driver)
      .option("query", buildQuery(config))
      .load()
    df
  }

  private def buildQuery(config: Config): String = {

    val reportTime = config.getDailyReportTime

    val agentCrmDomains = config
      .getList("agent_crm_domains", ",")
      .asScala
      .map(element => s"'$element'")
      .mkString(",")

    s"""
       |SELECT
       |    toInt64(received_at) as ${FieldConfig.TIMESTAMP},
       |    IF (JSONHas(context, 'userAgent'), JSONExtractString(context, 'userAgent'),'') as ${FieldConfig.USER_AGENT},
       |    IF (JSONHas(properties, 'url'), JSONExtractString(properties,'url'),'') as ${FieldConfig.URL},
       |    extract(IF(JSONHas(properties, 'page_location'), JSONExtractString(properties, 'page_location'),JSONExtractString(context,'page','path')), '(.+?)(\\?.+)?$$') as ${FieldConfig.PAGE},
       |    multiIf(type='page', 'page', event) AS ${FieldConfig.EVENT},
       |    toString(user_id) as ${FieldConfig.USER_ID}
       |FROM raw_data_normalized
       |WHERE 1=1
       |    AND channel='client'
       |    AND multiMatchAny(${FieldConfig.URL},[$agentCrmDomains])
       |    AND received_at >= $reportTime
       |    AND received_at < ${reportTime + 1.days.toMillis}
       |""".stripMargin
  }
}
