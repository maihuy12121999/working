package rever.rsparkflow.spark.extensions

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.client.RapIngestionClient
import rever.rsparkflow.spark.utils.{JsonUtils, TimestampUtils}

import scala.concurrent.duration.DurationInt

/**
  * @author anhlt (andy)
  * @since 11/05/2022
**/
trait RapIngestWriterMixin {

  def mergeIfRequired(config: Config, df: DataFrame, topic: String): Unit = {
    val mergeAfterWrite = config.getBoolean("merge_after_write", false)
    val isPresentOrFuture =
      TimestampUtils.asStartOfDay(config.getDailyReportTime) >= TimestampUtils.asStartOfDay(
        System.currentTimeMillis() - 1.days.toMillis
      )

    if (mergeAfterWrite && df.rdd.getNumPartitions > 0 && isPresentOrFuture) {
      RapIngestionClient.client(config).forceMerge(topic)
    }

  }

  def ingestDataframe(config: Config, dataFrame: DataFrame, topic: String)(
      converter: Row => JsonNode,
      batchSize: Int = 1000
  ): Long = {
    val begin = System.currentTimeMillis()
    val recordAccumulator = SparkSession.active.sparkContext.longAccumulator(s"rv_accumulate_counter_${topic}")
    recordAccumulator.reset()

    try {
      dataFrame.rdd.foreachPartition(partitionRows => {
        val client = RapIngestionClient.client(config)

        val records = partitionRows.map(converter).toSeq

        records
          .grouped(batchSize)
          .filterNot(_.isEmpty)
          .foreach(chunkRecords => {
            val insertRecord = client.ingest(topic, chunkRecords)
            if (insertRecord <= 0) {
              throw new Exception(
                s"Cannot save ${topic}: ${insertRecord}/${chunkRecords.size} records\n${JsonUtils.toJson(chunkRecords)}"
              )
            }
            recordAccumulator.add(chunkRecords.size)
          })
      })
      dataFrame.printSchema()
    } finally {
      println(s"Total record saved for ${topic}: ${recordAccumulator.value} records")
      println(
        s"Execution time to write dataframe to RAP: ${TimestampUtils.durationAsText((System.currentTimeMillis() - begin) * 1000L)}"
      )
    }

    recordAccumulator.value

  }

}
