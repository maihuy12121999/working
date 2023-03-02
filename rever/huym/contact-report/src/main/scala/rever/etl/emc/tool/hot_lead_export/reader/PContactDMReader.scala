package rever.etl.emc.tool.hot_lead_export.reader

import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.emc.tool.hot_lead_export.domain.HotLeadFields
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.domain.SparkSessionImplicits._
import rever.etl.rsparkflow.utils.TimestampUtils

class PContactDMReader extends SourceReader{
  override def read(tableName: String, config: Config): Dataset[Row] = {
    val driver = config.get("CH_DRIVER")
    val host = config.get("CH_HOST")
    val port = config.getInt("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val password = config.get("CH_PASSWORD")
    val (fromTime, toTime) = (
      TimestampUtils.parseMillsFromString(config.get("forcerun.from_date"), "yyyy-MM-dd"),
      TimestampUtils.parseMillsFromString(config.get("forcerun.to_date"), "yyyy-MM-dd")
    )

    SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port/datamart")
      .option("inferSchema", "true")
      .option("user", userName)
      .option("password", password)
      .option("driver", driver)
      .option("query", buildQuery(config,fromTime))
      .load()
      .withColumn("updated_time",col("updated_time").cast(LongType))
      .dropDuplicateCols(Seq(HotLeadFields.P_CID),col("updated_time").desc)
      .drop("updated_time")
  }
  def buildQuery(config: Config,reportTime:Long):String={
    val systemTags = config.get("system_tags","hot_lead")
    s"""
       |select
       |    p_cid,
       |    contact_status,
       |    replaceRegexpAll(coalesce(JSONExtractRaw(properties,'tags'),'[]'),'"','') as user_tags,
       |    replaceRegexpAll(coalesce(JSONExtractRaw(properties,'system_tags'),'[]'),'"','') as system_tags,
       |    updated_time
       |from contact_personal_contact
       |where 1=1
       |    and system_tags like '%$systemTags%'
       |    and created_time >= $reportTime
       |""".stripMargin
  }
}
