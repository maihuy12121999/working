package rever.rsparkflow.fs

import rever.rsparkflow.spark.domain.LocalFsConfig
import rever.rsparkflow.spark.utils.PathExtension.PathExtension
import rever.rsparkflow.spark.utils.{PathBuilder, PathExtension}
import software.amazon.awssdk.services.s3.model._
import vn.rever.jdbc.util.Utils.using

import java.io.{File, FileOutputStream, InputStream}

/**
  * @author anhlt - aka andy
 **/
case class LocalFsClient(config: LocalFsConfig) extends FsClient {

  def pathBuilder(extension: PathExtension = PathExtension.PARQUET): PathBuilder = {
    PathBuilder.build(config.parentPath, extension)
  }

  override def sparkFilePath(paths: String*): String = {
    Seq(paths.toSeq).flatten
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .mkString("/")
  }

  override def estimateSizeInBytes(bucketKey: String): Option[Long] = {
    val estimateSizeInBytes = getObjects(Some(bucketKey)).foldLeft(0L)((size, file) => {
      file.data.length() + size
    })

    Some(estimateSizeInBytes)
  }

  override def existBucket(bucketKey: String): Boolean = {
    try {
      val f = new File(bucketKey)
      f.exists()
    } catch {
      case ex: NoSuchKeyException => false
    }
  }

  override def existObject(objectKey: String): Boolean = {
    try {
      val f = new File(objectKey)
      f.exists()
    } catch {
      case ex: NoSuchKeyException => false
    }
  }

  override def listObjects(bucketKey: Option[String] = None): Seq[String] = {
    getObjects(bucketKey).map(_.data.getAbsolutePath)
  }

  private def getObjects(bucketKey: Option[String] = None): Seq[LocalFile] = {

    val file = new File(bucketKey.getOrElse(""))

    if (file.exists()) {
      file
        .listFiles()
        .map(file => LocalFile(file))
        .toSeq
    } else {
      Seq.empty
    }

  }

  override def writeObject(
      objectId: String,
      is: InputStream,
      contentLength: Option[Long] = None,
      contentType: Option[String] = None,
      metadata: Option[Map[String, String]] = None
  ): WriteObjectResult = {

    val file = new File(objectId)

    using(new FileOutputStream(file, false)) { out =>
      val buffer = Array.fill(2 * 1024 * 1024)(0.toByte)

      var lastRead: Long = 0
      do {
        lastRead = is.read(buffer)
        if (lastRead > 0) {
          out.write(buffer, 0, lastRead.toInt)
        }
      } while (lastRead != -1)
    }

    WriteObjectResult(
      etag = file.getAbsolutePath.hashCode.toString,
      path = Option(file.getParentFile).map(_.getAbsolutePath).getOrElse(""),
      objectId = file.getName
    )
  }

}
