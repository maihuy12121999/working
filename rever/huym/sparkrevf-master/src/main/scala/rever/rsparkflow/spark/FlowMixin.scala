package rever.rsparkflow.spark

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import rever.rsparkflow.fs.FsClient
import rever.rsparkflow.implicits.ConfigImplicits.ConfigImplicit
import rever.rsparkflow.implicits.SparkSessionImplicits.withPrintExecutionTime
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.utils.PathExtension.PathExtension
import rever.rsparkflow.spark.utils.{PathExtension, TimestampUtils}

import scala.concurrent.duration.DurationInt

/**
  * @author anhlt (andy)
  * @since 11/05/2022
**/
trait FlowMixin extends Serializable {

  private def readDataframe(
      sparkPaths: Seq[String],
      schema: Option[StructType],
      extension: PathExtension
  ): DataFrame = {
    val reader = SparkSession.active.read
      .option("mergeSchema", "true")
      .schema(schema.orNull)
    extension match {
      case PathExtension.CSV     => reader.option("header", "true").csv(sparkPaths: _*)
      case PathExtension.JSON    => reader.json(sparkPaths: _*)
      case PathExtension.PARQUET => reader.parquet(sparkPaths: _*)
    }
  }

  def loadAllA1DfFromS3(
      config: Config,
      jobId: String,
      fromTime: Long,
      toTime: Long,
      namePrefix: Option[String] = None,
      schema: Option[StructType] = None,
      extension: PathExtension = PathExtension.PARQUET
  ): Option[DataFrame] = {

    val fsClient = FsClient.client(config.fsConfig)

    val paths = getAllA1FileFromS3(
      config,
      jobId,
      fromTime,
      toTime,
      namePrefix
    ).map(fsClient.sparkFilePath(_))

    Option(paths)
      .filterNot(_.isEmpty)
      .map(readDataframe(_, schema, extension))

  }

  def loadAllAnDfFromS3(
      config: Config,
      jobId: String,
      fromTime: Long,
      toTime: Long,
      namePrefix: Option[String] = None,
      schema: Option[StructType] = None,
      extension: PathExtension = PathExtension.PARQUET
  ): Option[DataFrame] = {
    val fsClient = FsClient.client(config.fsConfig)

    val paths = getAllAnFileFromS3(config, jobId, fromTime, toTime, namePrefix)
      .map(fsClient.sparkFilePath(_))

    Option(paths)
      .filterNot(_.isEmpty)
      .map(readDataframe(_, schema, extension))

  }

  def getAllA1FileFromS3(
      config: Config,
      jobId: String,
      fromTime: Long,
      toTime: Long,
      namePrefix: Option[String] = None,
      extension: PathExtension = PathExtension.PARQUET
  ): Seq[String] = {

    val fsClient = FsClient.client(config.fsConfig)
    val pathBuilder = fsClient.pathBuilder(extension)

    val paths = TimestampUtils
      .makeDayRange(fromTime, toTime)
      .map(_._1)
      .map(time => pathBuilder.buildA1Path(jobId, time, namePrefix))
      .filter(fsClient.existBucket)

    println("A1 files:")
    paths.foreach(println)

    paths
  }

  def getAllAnFileFromS3(
      config: Config,
      jobId: String,
      fromTime: Long,
      toTime: Long,
      namePrefix: Option[String] = None,
      extension: PathExtension = PathExtension.PARQUET
  ): Seq[String] = {
    val fsClient = FsClient.client(config.fsConfig)
    val pathBuilder = fsClient.pathBuilder(extension)

    val paths = TimestampUtils
      .makeDayRange(fromTime, toTime)
      .map(_._1)
      .map(time => pathBuilder.buildAnPath(jobId, time, namePrefix))
      .filter(fsClient.existBucket)

    println("An files:")
    paths.foreach(println)

    paths
  }

  def getPreviousA0DfFromS3(
      jobId: String,
      reportTime: Long,
      config: Config,
      namePrefix: Option[String] = None,
      schema: Option[StructType] = None,
      extension: PathExtension = PathExtension.PARQUET
  ): Option[DataFrame] = {
    val fsClient = FsClient.client(config.fsConfig)
    val pathBuilder = fsClient.pathBuilder(extension)

    val previousReportTime = reportTime - 1.days.toMillis
    val previousA0Path = pathBuilder.buildA0Path(jobId, previousReportTime, namePrefix)

    val numOfPartition = fsClient.estimateNumOfPartitions(previousA0Path, 64 * 1024 * 1024, 8)

    if (fsClient.existBucket(previousA0Path)) {
      val path = fsClient.sparkFilePath(previousA0Path)
      Some(readDataframe(Seq(path), schema, extension).coalesce(numOfPartition))
    } else {
      println(s"No previous A0 was found: ${previousA0Path}")
      None
    }
  }

  def getPreviousYTDFromS3(
      jobId: String,
      reportTime: Long,
      config: Config,
      namePrefix: Option[String] = None,
      schema: Option[StructType] = None,
      extension: PathExtension = PathExtension.PARQUET
  ): Option[DataFrame] = {

    val previousReportTime = reportTime - 1.days.toMillis

    val fsClient = FsClient.client(config.fsConfig)
    val pathBuilder = fsClient.pathBuilder(extension)

    val previousPath = pathBuilder.buildYTDPath(jobId, previousReportTime, namePrefix)

    val numOfPartition = fsClient.estimateNumOfPartitions(previousPath, 64 * 1024 * 1024, 8)

    if (TimestampUtils.isSameYear(reportTime, previousReportTime) && fsClient.existBucket(previousPath)) {
      val path = fsClient.sparkFilePath(previousPath)

      Some(readDataframe(Seq(path), schema, extension).coalesce(numOfPartition))
    } else {
      println(s"No previous YTD was found: ${previousPath}")
      None
    }
  }

  def exportA1DfToS3(
      jobId: String,
      reportTime: Long,
      dailyDf: DataFrame,
      config: Config,
      namePrefix: Option[String] = None,
      extension: PathExtension = PathExtension.PARQUET
  ): Unit = {
    withPrintExecutionTime {

      val fsClient = FsClient.client(config.fsConfig)
      val pathBuilder = fsClient.pathBuilder(extension)

      val previousA1Path = pathBuilder.buildA1Path(jobId, reportTime - 1.days.toMillis, namePrefix)

      val numOfPartition = fsClient.estimateNumOfPartitions(previousA1Path, 64 * 1024 * 1024, 8)

      val path = pathBuilder.buildA1Path(jobId, reportTime, namePrefix)
      println(s"Export A1 to ${path} with $numOfPartition partitions")
      val writer = dailyDf
        .coalesce(numOfPartition)
        .write
        .mode(SaveMode.Overwrite)

      extension match {
        case PathExtension.CSV =>
          writer
            .option("header", "true")
            .csv(fsClient.sparkFilePath(path))
        case PathExtension.JSON =>
          writer.json(fsClient.sparkFilePath(path))
        case PathExtension.PARQUET =>
          writer.parquet(fsClient.sparkFilePath(path))
      }

    }
  }

  def exportA0DfToS3(
      jobId: String,
      reportTime: Long,
      df: DataFrame,
      config: Config,
      namePrefix: Option[String] = None,
      extension: PathExtension = PathExtension.PARQUET
  ): Unit = {
    withPrintExecutionTime {
      val fsClient = FsClient.client(config.fsConfig)
      val pathBuilder = fsClient.pathBuilder(extension)

      val previousPath = pathBuilder.buildA0Path(jobId, reportTime - 1.days.toMillis, namePrefix)

      val numOfPartition = fsClient.estimateNumOfPartitions(previousPath, 64 * 1024 * 1024, 8)

      val path = pathBuilder.buildA0Path(jobId, reportTime, namePrefix)

      println(s"Export A0 to ${path} with $numOfPartition partitions")
      val writer = df
        .coalesce(numOfPartition)
        .write
        .mode(SaveMode.Overwrite)

      extension match {
        case PathExtension.CSV =>
          writer.option("header", "true").csv(fsClient.sparkFilePath(path))
        case PathExtension.JSON =>
          writer.json(fsClient.sparkFilePath(path))
        case PathExtension.PARQUET =>
          writer.parquet(fsClient.sparkFilePath(path))
      }
    }
  }

  def exportAnDfToS3(
      jobId: String,
      reportTime: Long,
      df: DataFrame,
      config: Config,
      namePrefix: Option[String] = None,
      extension: PathExtension = PathExtension.PARQUET
  ): Unit = {
    withPrintExecutionTime {
      val fsClient = FsClient.client(config.fsConfig)
      val pathBuilder = fsClient.pathBuilder(extension)

      val previousPath = pathBuilder.buildAnPath(jobId, reportTime - 1.days.toMillis, namePrefix)

      val numOfPartition = fsClient.estimateNumOfPartitions(previousPath, 64 * 1024 * 1024, 8)

      val path = pathBuilder.buildAnPath(jobId, reportTime, namePrefix)
      println(s"Export An to ${path} with $numOfPartition partitions")
      val writer = df
        .coalesce(numOfPartition)
        .write
        .mode(SaveMode.Overwrite)

      extension match {
        case PathExtension.CSV =>
          writer.option("header", "true").csv(fsClient.sparkFilePath(path))
        case PathExtension.JSON =>
          writer.json(fsClient.sparkFilePath(path))
        case PathExtension.PARQUET =>
          writer.parquet(fsClient.sparkFilePath(path))
      }
    }
  }

  def exportYtdDfToS3(
      jobId: String,
      reportTime: Long,
      df: DataFrame,
      config: Config,
      namePrefix: Option[String] = None,
      extension: PathExtension = PathExtension.PARQUET
  ): Unit = {
    withPrintExecutionTime {
      val fsClient = FsClient.client(config.fsConfig)
      val pathBuilder = fsClient.pathBuilder(extension)

      val previousPath = pathBuilder.buildYTDPath(jobId, reportTime - 1.days.toMillis, namePrefix)

      val numOfPartition = fsClient.estimateNumOfPartitions(previousPath, 64 * 1024 * 1024, 8)

      val path = pathBuilder.buildYTDPath(jobId, reportTime, namePrefix)

      println(s"Export YTD to ${path} with $numOfPartition partitions")
      val writer = df
        .coalesce(numOfPartition)
        .write
        .mode(SaveMode.Overwrite)
      extension match {
        case PathExtension.CSV =>
          writer.option("header", "true").csv(fsClient.sparkFilePath(path))
        case PathExtension.JSON =>
          writer.json(fsClient.sparkFilePath(path))
        case PathExtension.PARQUET =>
          writer.parquet(fsClient.sparkFilePath(path))
      }
    }
  }

}
