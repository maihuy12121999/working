package rever.rsparkflow.dao

import rever.rsparkflow.dao.clickhouse.CustomClickhouseDAO
import rever.rsparkflow.spark.domain.ClickHouseConfig
import rever.rsparkflow.spark.utils.DataSourceUtils
import vn.rever.jdbc.JdbcRecord

import javax.sql.DataSource

object DAOFactory {
  private val clientMap = scala.collection.mutable.Map.empty[String, CustomClickhouseDAO[_ <: JdbcRecord]]

  def singletonClickhouse[T <: JdbcRecord](config: ClickHouseConfig)(
      fn: DataSource => CustomClickhouseDAO[T]
  ): CustomClickhouseDAO[T] = {
    val cacheKey = s"${config.host}:${config.port}-${config.dbName.getOrElse("")}.${config.tableName.getOrElse(
      ""
    )}"
    clientMap.get(cacheKey) match {
      case Some(client) => client.asInstanceOf[CustomClickhouseDAO[T]]
      case None =>
        clientMap.synchronized {
          clientMap
            .get(cacheKey)
            .fold[CustomClickhouseDAO[T]]({
              val ds = DataSourceUtils.getClickhouseDataSource(config)
              val client = fn(ds)
              clientMap.put(cacheKey, client)
              client
            })(x => x.asInstanceOf[CustomClickhouseDAO[T]])

        }
    }
  }
}
