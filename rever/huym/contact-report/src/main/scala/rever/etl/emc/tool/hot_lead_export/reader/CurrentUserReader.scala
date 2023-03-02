package rever.etl.emc.tool.hot_lead_export.reader

import rever.etl.emc.tool.hot_lead_export.domain.HotLeadFields
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.domain.SparkSessionImplicits._

class CurrentUserReader extends SourceReader{
  override def read(tableName: String, config: Config): Dataset[Row] = {
    val driver = config.get("CH_DRIVER")
    val host = config.get("CH_HOST")
    val port = config.getInt("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val password = config.get("CH_PASSWORD")
    val reportTime = config.getDailyReportTime

    SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port/datamart")
      .option("inferSchema", "true")
      .option("user", userName)
      .option("password", password)
      .option("driver", driver)
      .option("query", buildQuery(config,reportTime))
      .load()
      .withColumn("updated_time",col("updated_time").cast(LongType))
      .dropDuplicateCols(Seq("username"),col("updated_time").desc)
      .drop("updated_time")
  }
  def buildQuery(config: Config,reportTime:Long):String={
    s"""
       |select
       |    username,
       |    work_email,
       |    dictGetOrDefault('default.dict_team', 'name', tuple(assumeNotNull(team)), '') as team_name,
       |    updated_time
       |from rever_profile_user
       |""".stripMargin
  }
}
