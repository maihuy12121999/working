package rever.etl.support.cs2_call_cta_analysis.reader

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.rsparkflow.spark.api.SourceReader
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.utils.JsonUtils
import rever.etl.support.cs2_call_cta_analysis.CallCtaHelper

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.concurrent.duration.DurationInt

class Call3cxExtensionReader extends SourceReader {

  override def read(tableName: String, config: Config): Dataset[Row] = {
    val driver = config.get("MYSQL_DRIVER")
    val host = config.get("MYSQL_HOST")
    val port = config.getInt("MYSQL_PORT")
    val userName = config.get("MYSQL_USER_NAME")
    val password = config.get("MYSQL_PASSWORD")

    val df = SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:mysql://$host:$port/rever-call")
      .options(Map("delimiter" -> ",", "header" -> "true", "inferSchema" -> "true"))
      .option("user", userName)
      .option("password", password)
      .option("driver", driver)
      .option("query", buildQuery(config))
      .load()
      .flatMap(flattenExtensionPhone(_, CallCtaHelper.callExtensionSchema))(
        RowEncoder(CallCtaHelper.callExtensionSchema)
      )

    val reportTime = config.getDailyReportTime
    df.where(col("assigned_time").lt(reportTime + 1.days.toMillis))
      .orderBy(col("assigned_time").desc)
      .dropDuplicates("extension")

  }

  private def buildQuery(config: Config): String = {

    val query = s"""
       |SELECT
       |  ext_id AS extension,
       |  call_service,
       |  info,
       |  creator,
       |  timestamp,
       |  last_active_time
       |FROM call_extension
       |WHERE 1 = 1
       |""".stripMargin

    query
  }

  private def flattenExtensionPhone(row: Row, schema: StructType): Iterator[Row] = {

    val extension = row.getAs[String]("extension")
    val infoNode = row.getAs[String]("info") match {
      case null => JsonUtils.fromJson[JsonNode]("{}")
      case x    => JsonUtils.fromJson[JsonNode](x)
    }
    val callService = row.getAs[String]("call_service")
    val creator = row.getAs[String]("creator")
    val timestamp = row.getAs[Long]("timestamp")

    parse3cxAssignHistories(timestamp, infoNode)
      .filter(_._2.nonEmpty)
      .map { case (assignTime, phone) =>
        new GenericRowWithSchema(
          Array(assignTime, extension, callService, phone, creator),
          schema
        )
      }
      .toIterator

  }

  private def parse3cxAssignHistories(timestamp: Long, infoNode: JsonNode): Map[Long, String] = {
    val assignHistoryMap = scala.collection.mutable.HashMap.empty[Long, String]
    infoNode.at("/phone").asText("") match {
      case phone if phone.nonEmpty => assignHistoryMap.put(timestamp, phone)
      case _ =>
        infoNode
          .at("/list_did_assign")
          .elements()
          .asScala
          .foreach { node =>
            val phone = node.at("/did").asText("")
            val assignTime = node.at("/assign_time").asLong(timestamp)

            assignHistoryMap.put(assignTime, phone)
          }
    }

    assignHistoryMap.toMap
  }

}
