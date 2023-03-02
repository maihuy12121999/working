package rever.rsparkflow.spark.example.writer

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, Row}
import rever.rsparkflow.spark.api.SinkWriter
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.client.RapIngestionClient
import rever.rsparkflow.spark.utils.JsonUtils

import scala.collection.mutable.ListBuffer

class DataIngestionWriter extends SinkWriter {

  def write(
      tableName: String,
      dataFrame: DataFrame,
      config: Config
  ): DataFrame = {

    dataFrame.foreachPartition { (rows: Iterator[Row]) =>
      val client = RapIngestionClient.client(config)
      val buffer = ListBuffer.empty[JsonNode]
      rows.foreach { row =>
        buffer.append(JsonUtils.fromJson[JsonNode](row.prettyJson))

        if (buffer.size >= 500) {
          val insertCount = client.ingest("rap.analytics.test_from_spark_job", buffer)
          println(s"Insert:  ${insertCount}")
          buffer.clear()
        }
      }

      if (buffer.nonEmpty) {
        val insertCount = client.ingest("rap.analytics.test_from_spark_job", buffer)
        println(s"Insert:  ${insertCount}")
      }

    }
    dataFrame
  }
}
