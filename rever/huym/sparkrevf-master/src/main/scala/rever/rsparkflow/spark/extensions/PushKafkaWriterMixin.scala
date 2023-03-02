package rever.rsparkflow.spark.extensions

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.client.{PushKafkaClient, PushKafkaRecord}
import rever.rsparkflow.spark.utils.{JsonUtils, TimestampUtils}

/**
  * @author anhlt (andy)
  * @since 11/05/2022
**/
trait PushKafkaWriterMixin {

  def pushDataframeToKafka(config: Config, dataFrame: DataFrame, topic: String)(
      converter: Row => PushKafkaRecord,
      batchSize: Int = 800
  ): Long = {
    val begin = System.currentTimeMillis()
    val recordCounter = SparkSession.active.sparkContext.longAccumulator(s"rv_push_kafka_counter_${topic}")
    recordCounter.reset()

    try {
      dataFrame.rdd.foreachPartition(partitionRows => {
        val client = PushKafkaClient.client(config)

        val records = partitionRows.map(converter).toSeq

        records
          .grouped(batchSize)
          .filterNot(_.isEmpty)
          .foreach(chunkRecords => {
            val insertRecord = client.multiPushKafka(topic, chunkRecords)
            if (insertRecord <= 0) {
              throw new Exception(
                s"Cannot push to Kafka ${topic}: ${insertRecord}/${chunkRecords.size} records\nTop 5: ${JsonUtils
                  .toJson(chunkRecords.take(5))}"
              )
            }
            recordCounter.add(chunkRecords.size)
          })
      })
      dataFrame.printSchema()
    } finally {
      println(s"Total: ${recordCounter.value} records pushed to Kafka for topic=${topic}")
      println(
        s"Execution time to write dataframe to Kafka: ${TimestampUtils
          .durationAsText((System.currentTimeMillis() - begin) * 1000L)}"
      )
    }

    recordCounter.value

  }

}
