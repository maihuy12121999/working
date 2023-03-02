package rever.etl.data_sync.core.mysql

import com.zaxxer.hikari.HikariDataSource
import rever.etl.data_sync.core.Sink
import rever.etl.data_sync.domain.GenericRecord
import rever.etl.rsparkflow.api.configuration.Config
import vn.rever.jdbc.mysql.MysqlEngine

import java.util.concurrent.atomic.LongAdder
import javax.sql.DataSource
import scala.collection.mutable.ListBuffer

object MySqlSink {
  private val dsMap = scala.collection.mutable.Map.empty[String, DataSource]

  def client(config: Config): DataSource = {
    val host = config.get("MYSQL_HOST")
    val port = config.getInt("MYSQL_PORT")
    val db = config.get("MYSQL_DB")

    val cacheKey = s"${host}_${port}_${db}"

    dsMap.get(cacheKey) match {
      case Some(client) => client
      case None =>
        dsMap.synchronized {
          dsMap
            .get(cacheKey)
            .fold[DataSource]({
              val ds = getDataSource(config)
              dsMap.put(cacheKey, ds)
              ds
            })(x => x)

        }
    }
  }

  private def getDataSource(config: Config): HikariDataSource = {
    val db = config.get("MYSQL_DB")
    val ds = MysqlEngine
      .getDataSource(
        driver = config.get("MYSQL_DRIVER"),
        host = config.get("MYSQL_HOST"),
        port = config.getInt("MYSQL_PORT"),
        user = config.get("MYSQL_USER_NAME"),
        password = config.get("MYSQL_PASSWORD"),
        dbName = db
      )
      .asInstanceOf[HikariDataSource]

    val timeoutMs = config.getLong("MYSQL_TIMEOUT_MS", 30000L)
    ds.addDataSourceProperty("socket_timeout", timeoutMs)
    ds.addDataSourceProperty("connection_timeout", timeoutMs)
    ds.setValidationTimeout(java.util.concurrent.TimeUnit.SECONDS.toMillis(60))
    ds
  }
}

case class MySqlSink(
    config: Config,
    tableName: String,
    primaryKeys: Seq[String],
    fields: Seq[String],
    batchSize: Int
) extends Sink[Map[String, Any]] {

  private val genericRecordDAO = MySQLGenericRecordDAO(
    MySqlSink.client(config),
    tableName,
    primaryKeys,
    fields
  )

  private val buffer = ListBuffer.empty[Map[String, Any]]

  private val counter = new LongAdder()

  override def write(record: Map[String, Any]): Int = {
    buffer.append(record)

    if (buffer.size >= batchSize) {
      val count = flushBatch(buffer)
      buffer.clear()
      count
    } else {
      1
    }
  }

  override def writeBatch(records: Seq[Map[String, Any]]): Int = {
    records.foreach(write)

    records.size
  }

  private def flushBatch(records: Seq[Map[String, Any]]): Int = {
    val chRecords = records.map(record => {
      GenericRecord(primaryKeys, fields, record)
    })

    val count = genericRecordDAO.multiInsert(chRecords)
    if (count <= 0 && records.nonEmpty) {
      throw new Exception(s"Failed to ingest data: ${tableName} - ${count}/${records.size}")
    }
    println(s"Sink consumed: ${tableName} - ${records.size}/${counter.longValue()}")
    counter.add(records.size)
    count
  }

  override def end(): Unit = {
    if (buffer.nonEmpty) {
      writeBatch(buffer)
      buffer.clear()
    }
  }

}
