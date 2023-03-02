package rever.rsparkflow.spark.domain

case class ClickHouseConfig(
    driver: String,
    host: String,
    port: Int,
    userName: String,
    password: String,
    dbName: Option[String],
    tableName: Option[String],
    properties: Option[Map[String, Any]]
)
