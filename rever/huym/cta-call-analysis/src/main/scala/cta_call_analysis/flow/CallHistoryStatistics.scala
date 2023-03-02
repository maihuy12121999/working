package cta_call_analysis.flow

import cta_call_analysis.reader.{CallHistoryReader, CsvCallHistoryReader}
import cta_call_analysis.writer.{TestCallHistoryWriter, TestUserEventWriter}
import org.apache.spark.sql.functions.{col, date_format, udf}
import org.apache.spark.sql.types.{DateType, LongType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class CallHistoryStatistics extends FlowMixin{
  @Output(writer = classOf[TestCallHistoryWriter])
  @Table("CallHistoryData")
  def build(
           @Table(
             name = "log.call_history_1",
             reader =classOf[CallHistoryReader])
           callHistoryData:DataFrame,
           config:Config
           ):DataFrame={
    val castLongTypeDF = callHistoryData
      .withColumn("duration", col("duration").cast(LongType))
      .withColumn("start_time", col("start_time").cast(LongType))
      .withColumn("end_time", col("end_time").cast(LongType))
    castLongTypeDF.printSchema()
    castLongTypeDF

  }
}
