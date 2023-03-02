package rever.rsparkflow.implicits

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory
import rever.rsparkflow.implicits.ConfigImplicits.ConfigImplicit
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.utils.TimestampUtils

import scala.concurrent.duration.DurationLong

/**
  * @author anhlt (andy)
  * @since 02/07/2022
  * */
object SparkSessionImplicits {
  private lazy val logger = LoggerFactory.getLogger(classOf[SparkSessionImplicits])

  def withPrintExecutionTime[T](fn: => T): T = {

    val start = System.currentTimeMillis()

    try {
      fn
    } finally {
      val duration = (System.currentTimeMillis() - start).millis.toMicros

      println(s"Execution time is: ${TimestampUtils.durationAsText(duration)}")

    }

  }

  def readJDBCByTimeChunks(fromTime: Long, toTime: Long, step: Long)(readerFn: (Long, Long) => DataFrame): DataFrame = {
    val dfList = TimestampUtils
      .chunkTimeRange(fromTime, toTime, step)
      .map { case (start, end) => readerFn(start, end - 1) }

    dfList.unionAllByName()
  }

  implicit class SparkSessionImplicits(val sparkSession: SparkSession) extends AnyVal {

    def initS3(config: Config): Unit = {
      config.optS3Config match {
        case Some(config) =>
          sparkSession.sparkContext.hadoopConfiguration.set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
          )
          sparkSession.sparkContext.hadoopConfiguration
            .set("fs.s3a.access.key", config.accessKey)
          sparkSession.sparkContext.hadoopConfiguration
            .set("fs.s3a.secret.key", config.secretKey)
          sparkSession.sparkContext.hadoopConfiguration
            .set("fs.s3a.endpoint", "s3.amazonaws.com")
        case None =>
          logger.info("No S3 config was configured.")
      }
    }
  }

  implicit class RowSeqImplicit(val rows: Seq[Row]) extends AnyVal {
    def getFields[T](field: String): Seq[T] = {
      rows.map(row => row.getAs[T](field)).filterNot(_ == null)
    }
  }

  implicit class DataFrameImplicit(val df: DataFrame) extends AnyVal {

    def dropDuplicateCols(cols: Seq[String], orderBy: Column*): DataFrame = {
      val dropDuplicateWinSpec = Window
        .partitionBy(cols.head, cols.tail: _*)
        .orderBy(orderBy: _*)
      df.withColumn("__index", row_number.over(dropDuplicateWinSpec))
        .where(col("__index").equalTo(1))
        .drop(col("__index"))
    }
  }

  implicit class DataFrameListImplicit(val dfList: Seq[DataFrame]) extends AnyVal {

    def unionAllByName(): DataFrame = {
      dfList.size match {
        case size if size >= 2 =>
          val headDf = dfList.head
          dfList.tail.foldLeft(headDf)((resultDf, df) => resultDf.unionByName(df, true))
        case _ => dfList.head
      }
    }
  }
}
