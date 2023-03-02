package rever.rsparkflow.spark.utils

import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.codec.CharEncoding
import org.apache.commons.io.FileUtils
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.domain.ClickHouseConfig
import vn.rever.jdbc.clickhouse.ClickHouseEngine
import vn.rever.jdbc.mysql.MysqlEngine
import vn.rever.jdbc.util.Utils.using

import java.io.File
import javax.sql.DataSource
import scala.collection.JavaConverters._

object DataSourceUtils {

  private val clickhouseEscapeMapping = Map(
    '\\' -> "\\\\",
    '\n' -> "\\n",
    '\t' -> "\\t",
    '\b' -> "\\b",
    '\f' -> "\\f",
    '\r' -> "\\r",
    '\0' -> "\\0",
    '\'' -> "\\'",
    '`' -> "\\`"
  )

  def initMysql(ds: DataSource, sqlFiles: Seq[String]): Unit = {
    using(ds.getConnection) { conn =>
      loadAllSQLFiles(sqlFiles).foreach(file => {
        val sql =
          FileUtils
            .readLines(file, CharEncoding.UTF_8)
            .asScala
            .map(_.trim)
            .filter(_.nonEmpty)
            .mkString("\n")
        using(conn.prepareStatement(sql)) { ps =>
          ps.execute()
        }
      })
    }
  }

  def initClickhouse(ds: DataSource, sqlFiles: Seq[String]): Unit = {

    loadAllSQLFiles(sqlFiles).foreach(file => {
      using(ds.getConnection) { conn =>
        val lines = FileUtils.readLines(file, CharEncoding.UTF_8).asScala.map(_.trim).filter(_.nonEmpty)
        ClickHouseEngine
          .detachSqlToSingleSql(lines)
          .foreach(sql => {
            println(s"""
                 |$sql
                 |-------------------
                 |""".stripMargin)
            using(conn.prepareStatement(sql))(_.execute())
          })
      }
    })

  }

  def initClickhouse(ds: DataSource, sql: String): Unit = {

    using(ds.getConnection) { conn =>
      println(s"""
                 |$sql
                 |-------------------
                 |""".stripMargin)
      using(conn.prepareStatement(sql))(_.execute())
    }

  }

  def getMySQLDataSource(config: Config, dbName: String): HikariDataSource = {
    val ds = MysqlEngine
      .getDataSource(
        driver = config.get("driver"),
        host = config.get("host"),
        port = config.getInt("port"),
        user = config.get("user"),
        password = config.get("pass"),
        dbName = dbName
      )
      .asInstanceOf[HikariDataSource]
    ds.addDataSourceProperty("allowMultiQueries", "true")
    ds.addDataSourceProperty("allowPublicKeyRetrieval", "true")

    ds
  }

  def getClickhouseDataSource(config: ClickHouseConfig): HikariDataSource = {

    val ds = ClickHouseEngine
      .getDataSource(
        host = config.host,
        port = config.port,
        user = config.userName,
        password = config.password,
        dbName = config.dbName.getOrElse("")
      )
      .asInstanceOf[HikariDataSource]

    ds.addDataSourceProperty("socket_timeout", 60000L)
    ds.addDataSourceProperty("connection_timeout", 60000L)
    config.properties.getOrElse(Map.empty[String, String]).foreach {
      case (k, v) =>
        ds.addDataSourceProperty(k, v)
    }

    ds.setValidationTimeout(java.util.concurrent.TimeUnit.SECONDS.toMillis(60))
    ds
  }

  private def loadAllSQLFiles(filePaths: Seq[String]): Seq[File] = {
    filePaths
      .map(new File(_))
      .flatMap(file =>
        if (file.isDirectory) {
          FileUtils.listFiles(file, Array("sql"), true).asScala.toArray.toSeq
        } else {
          Seq(file)
        }
      )
  }

  def mysqlEscapeStr(sqlStr: String): String = {

    if (sqlStr != null) {
      sqlStr
        .replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\0", "\\0")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\"", "\\\"")
        .replace("\\x1a", "\\Z")
    } else {
      sqlStr
    }
  }

  def clickHouseEscape(s: String): String = {
    if (s == null) return "\\N"
    val sb = new StringBuilder
    for (i <- 0 until s.length) {
      val ch = s.charAt(i)
      val escaped = clickhouseEscapeMapping.get(ch)
      if (escaped != null) sb.append(escaped)
      else sb.append(ch)
    }
    sb.toString
  }

}
