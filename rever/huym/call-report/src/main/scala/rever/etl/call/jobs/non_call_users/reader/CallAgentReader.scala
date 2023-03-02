package rever.etl.call.jobs.non_call_users.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

import scala.concurrent.duration.DurationInt

class CallAgentReader extends SourceReader{


  override def read(tableName: String, config: Config): Dataset[Row] = {
    val host = config.get("CH_HOST")
    val driver = config.get("CH_DRIVER")
    val port = config.get("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val passWord = config.get("CH_PASSWORD")

    SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port/datamart")
      .option("user", userName)
      .option("password", passWord)
      .option("driver", driver)
      .option("query", initQuery(config))
      .load()

  }

  def initQuery(config: Config):String={
    val reportTime = config.getDailyReportTime
    s"""
       |select
       |   agent_id,
       |   call_service
       |from
       |    (select
       |        agent_id,
       |        call_service
       |    from call_history
       |    where 1=1
       |        and direction = 'outbound'
       |        and time >=$reportTime
       |        and time < ${reportTime+1.days.toMillis})
       |group by agent_id,call_service
       |""".stripMargin
  }
}
