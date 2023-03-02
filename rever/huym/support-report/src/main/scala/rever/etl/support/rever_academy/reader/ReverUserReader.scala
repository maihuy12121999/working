package rever.etl.support.rever_academy.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.rsparkflow.spark.api.SourceReader
import rever.rsparkflow.spark.api.configuration.Config

class ReverUserReader extends SourceReader {

  override def read(s: String, config: Config): Dataset[Row] = {
    val driver = config.get("CH_DRIVER")
    val host = config.get("CH_HOST")
    val port = config.getInt("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val password = config.get("CH_PASSWORD")

    val df = SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port/airbyte")
      .option("inferSchema", "true")
      .option("user", userName)
      .option("password", password)
      .option("driver", driver)
      .option("query", buildQuery(config))
      .load()

    df
  }

  private def buildQuery(config: Config): String = {
    s"""
       |
       |SELECT
       |    work_email,
       |    full_name,
       |    employee_id,
       |    birthday,
       |    job_title_id,
       |    '' AS job_title,
       |    hire_date,
       |    line_manager,
       |    line_manager_email,
       |    team,
       |    '' AS department,
       |    office,
       |    status,
       |    if(employment_status IS NULL,'[]', employment_status) AS employment_status
       |FROM(
       |    SELECT 
       |        JSONExtractString(_airbyte_data,'properties' ) AS properties,
       |        JSONExtractString(_airbyte_data, 'work_email') AS work_email,
       |        JSONExtractString(_airbyte_data, 'full_name') AS full_name,
       |        JSONExtractString(_airbyte_data, 'employee_id') AS employee_id,
       |        JSONExtractString(properties, 'birthday' ) AS birthday,
       |        JSONExtractString(properties, 'job_title') AS job_title_id,
       |        JSONExtractString(_airbyte_data, 'hire_date') AS hire_date,
       |        JSONExtractString(_airbyte_data, 'job_report_to' ) AS line_manager_id,
       |        dictGetOrDefault('default.dict_staff', 'work_email', tuple(assumeNotNull(line_manager_id)), '') AS line_manager_email,
       |        dictGetOrDefault('default.dict_staff', 'full_name', tuple(assumeNotNull(line_manager_id)), '') AS line_manager,
       |        JSONExtractString(_airbyte_data, 'office') AS office_id,
       |        JSONExtractString(_airbyte_data, 'team') AS team_id,
       |        dictGetOrDefault('default.dict_team', 'name', tuple(assumeNotNull(team_id)), '') AS team,
       |        dictGetOrDefault('default.dict_office', 'name', tuple(assumeNotNull(office_id)), '') AS office,
       |        JSONExtractInt(_airbyte_data,'status' ) AS status,
       |        JSONExtractRaw(properties, 'employment_status') AS employment_status
       |    FROM _airbyte_raw_rever_profile_user
       |    WHERE 1=1
       |)
       |""".stripMargin
  }
}
