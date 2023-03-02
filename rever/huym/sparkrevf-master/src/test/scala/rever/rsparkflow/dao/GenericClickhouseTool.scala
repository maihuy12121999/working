package rever.rsparkflow.dao

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import rever.rsparkflow.spark.domain.ClickHouseConfig
import rever.rsparkflow.spark.utils.DataSourceUtils

class GenericClickhouseTool extends FunSuite with BeforeAndAfterAll {

  private val dataSource = DataSourceUtils.getClickhouseDataSource(
    ClickHouseConfig(
      driver = "",
      host = "127.0.0.1",
      port = 8123,
      userName = "default",
      password = "",
      None,
      None,
      properties = None
    )
  )

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    DataSourceUtils.initClickhouse(dataSource, "CREATE DATABASE rsparkflow_test")
    DataSourceUtils.initClickhouse(
      dataSource,
      """
        |CREATE TABLE rsparkflow_test.users (
        | user_id String,
        | name String,
        | log_time Int64
        |)
        |ENGINE = ReplacingMergeTree(log_time)
        |PRIMARY KEY (user_id)
        |ORDER BY (user_id)
        |SETTINGS index_granularity = 8192;
        |""".stripMargin
    )
  }

  override protected def afterAll(): Unit = {
    super.afterAll()

    dataSource.close()
  }

  test("Write data to CH") {}

}
