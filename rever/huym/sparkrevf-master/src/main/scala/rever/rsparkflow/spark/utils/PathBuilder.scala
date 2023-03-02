package rever.rsparkflow.spark.utils

import rever.rsparkflow.spark.utils.PathExtension.PathExtension

/**
  * @author anhlt (andy)
  * @since 21/04/2022
**/

object PathExtension extends Enumeration {
  type PathExtension = PathExtension.Value
  final val CSV = Value(1, "csv")
  final val JSON = Value(2, "json")
  final val PARQUET = Value(3, "parquet")
}

trait PathBuilderFactory {
  def csv(parentPath: String): PathBuilder

  def json(parentPath: String): PathBuilder

  def parquet(prefixPath: String): PathBuilder
}

object PathBuilder extends PathBuilderFactory {

  def build(parentPath: String, extension: PathExtension): PathBuilder = {
    extension match {
      case PathExtension.CSV     => csv(parentPath)
      case PathExtension.JSON    => json(parentPath)
      case PathExtension.PARQUET => parquet(parentPath)
      case _                     => throw new Exception(s"Unknown extension ${extension}")
    }
  }

  def csv(parentPath: String): PathBuilder = PathBuilderImpl("csv", parentPath)

  def json(parentPath: String): PathBuilder = PathBuilderImpl("json", parentPath)

  def parquet(prefixPath: String): PathBuilder = PathBuilderImpl("parquet", prefixPath)
}

@deprecated("Consider to use PathBuilder instead. This requires to migrate old data.")
object PathBuilderFactory extends PathBuilderFactory {
  def csv(parentPath: String): PathBuilder = PathBuilderImpl2("csv", parentPath)

  def json(parentPath: String): PathBuilder = PathBuilderImpl2("json", parentPath)

  def parquet(prefixPath: String): PathBuilder = PathBuilderImpl2("parquet", prefixPath)
}

trait PathBuilder {

  val extension: String

  def buildAnPath(jobId: String, time: Long, namePrefix: Option[String] = None): String

  def buildA0Path(jobId: String, time: Long, namePrefix: Option[String] = None): String

  def buildA1Path(jobId: String, time: Long, namePrefix: Option[String] = None): String

  def buildA7Path(jobId: String, time: Long, namePrefix: Option[String] = None): String

  def buildA30Path(jobId: String, time: Long, namePrefix: Option[String] = None): String

  def buildWTDPath(jobId: String, time: Long, namePrefix: Option[String]): String

  def buildMTDPath(jobId: String, time: Long, namePrefix: Option[String] = None): String

  def buildQTDPath(jobId: String, time: Long, namePrefix: Option[String] = None): String

  def buildYTDPath(jobId: String, time: Long, namePrefix: Option[String] = None): String

  def buildAxPath(
      `type`: String,
      parentPath: String,
      jobId: String,
      time: Long,
      namePrefix: Option[String] = None
  ): String

  def getPathSegments(filePath: String): Seq[String] = {
    filePath.split("/", -1).toSeq
  }
}

case class PathBuilderImpl(extension: String, parentPath: String) extends PathBuilder {

  /**
    * Build the A1 path for the given job in the given day
    *
    * - The name of file/folder will be: [$namePrefix_]dd_MM_yyyy.$extension
    *
    * - The output will be: $parentPath/yyyy/MM/[$namePrefix_]dd_MM_yyyy.$extension
    * @param jobId
    * @param time
    * @param namePrefix
    * @return the A1 path
    */
  override def buildA1Path(jobId: String, time: Long, namePrefix: Option[String] = None): String = {
    buildAxPath("A1", parentPath, jobId, time, namePrefix)
  }

  override def buildA7Path(jobId: String, time: Long, namePrefix: Option[String] = None): String = {
    buildAxPath("A7", parentPath, jobId, time, namePrefix)
  }

  override def buildA30Path(jobId: String, time: Long, namePrefix: Option[String] = None): String = {
    buildAxPath("A30", parentPath, jobId, time, namePrefix)
  }

  override def buildA0Path(jobId: String, time: Long, namePrefix: Option[String] = None): String = {
    buildAxPath("A0", parentPath, jobId, time, namePrefix)
  }

  override def buildAnPath(jobId: String, time: Long, namePrefix: Option[String] = None): String = {
    buildAxPath("An", parentPath, jobId, time, namePrefix)
  }

  override def buildWTDPath(jobId: String, time: Long, namePrefix: Option[String]): String = {
    buildAxPath("WTD", parentPath, jobId, time, namePrefix)
  }

  override def buildMTDPath(jobId: String, time: Long, namePrefix: Option[String]): String = {
    buildAxPath("MTD", parentPath, jobId, time, namePrefix)
  }

  override def buildQTDPath(jobId: String, time: Long, namePrefix: Option[String]): String = {
    buildAxPath("QTD", parentPath, jobId, time, namePrefix)
  }

  override def buildYTDPath(jobId: String, time: Long, namePrefix: Option[String]): String = {
    buildAxPath("YTD", parentPath, jobId, time, namePrefix)
  }

  final def buildAxPath(
      `type`: String,
      parentPath: String,
      jobId: String,
      time: Long,
      namePrefix: Option[String] = None
  ): String = {
    val yearAndMonth = TimestampUtils.format(time, Some("yyyy/MM"))
    val dateAsString = TimestampUtils.format(time, Some("dd_MM_yyyy"))
    val fileName = Seq(
      namePrefix,
      Some(dateAsString)
    ).flatten.mkString("_")

    Seq(parentPath, `type`, jobId, yearAndMonth, s"$fileName.${extension}")
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .mkString("/")
  }
}

case class PathBuilderImpl2(extension: String, parentPath: String) extends PathBuilder {

  override def buildA1Path(jobId: String, time: Long, namePrefix: Option[String] = None): String = {
    buildAxPath("A1", parentPath, jobId, time, namePrefix)
  }

  override def buildA7Path(jobId: String, time: Long, namePrefix: Option[String] = None): String = {
    buildAxPath("A7", parentPath, jobId, time, namePrefix)
  }

  override def buildA30Path(jobId: String, time: Long, namePrefix: Option[String] = None): String = {
    buildAxPath("A30", parentPath, jobId, time, namePrefix)
  }

  override def buildA0Path(jobId: String, time: Long, namePrefix: Option[String] = None): String = {
    buildAxPath("A0", parentPath, jobId, time, namePrefix)
  }

  override def buildAnPath(jobId: String, time: Long, namePrefix: Option[String] = None): String = {
    buildAxPath("An", parentPath, jobId, time, namePrefix)
  }

  override def buildWTDPath(jobId: String, time: Long, namePrefix: Option[String]): String = {
    buildAxPath("WTD", parentPath, jobId, time, namePrefix)
  }

  override def buildMTDPath(jobId: String, time: Long, namePrefix: Option[String]): String = {
    buildAxPath("MTD", parentPath, jobId, time, namePrefix)
  }

  override def buildQTDPath(jobId: String, time: Long, namePrefix: Option[String]): String = {
    buildAxPath("QTD", parentPath, jobId, time, namePrefix)
  }

  override def buildYTDPath(jobId: String, time: Long, namePrefix: Option[String]): String = {
    buildAxPath("YTD", parentPath, jobId, time, namePrefix)
  }

  def buildAxPath(
      `type`: String,
      parentPath: String,
      jobId: String,
      time: Long,
      namePrefix: Option[String] = None
  ): String = {
    val yearMonthAndDay = TimestampUtils.format(time, Some("yyyy/MM/dd"))
    val dateAsString = TimestampUtils.format(time, Some("dd_MM_yyyy"))
    val fileName = Seq(
      namePrefix,
      Some(dateAsString)
    ).flatten.mkString("_")

    Seq(parentPath, `type`, jobId, yearMonthAndDay, s"$fileName.${extension}")
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .mkString("/")
  }
}
