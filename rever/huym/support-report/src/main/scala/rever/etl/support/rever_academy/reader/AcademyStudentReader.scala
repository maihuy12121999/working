package rever.etl.support.rever_academy.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.rsparkflow.spark.api.SourceReader
import rever.rsparkflow.spark.api.configuration.Config

class AcademyStudentReader extends SourceReader {

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
      .option("query", buildQuery())
      .load()

    df
  }

  private def buildQuery(): String = {
    s"""
       |
       |SELECT
       |      IF (JSONHas(_airbyte_data,'Họ tên'), JSONExtractString(_airbyte_data,'Họ tên'),'') as name,
       |      IF (JSONHas(_airbyte_data,'Email'), JSONExtractString(_airbyte_data,'Email'),'') as email,
       |      IF (JSONHas(_airbyte_data,'Thời gian'), JSONExtractString(_airbyte_data,'Thời gian'),'') as course
       |from airbyte._airbyte_raw_rever_academy_Danh_sach
       |where 1=1
       |     AND name <> ''
       |     AND email <> ''
       |     AND course <> ''
       |""".stripMargin
  }
}
