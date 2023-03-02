package rever.rsparkflow.fs

import org.slf4j.{Logger, LoggerFactory}
import rever.rsparkflow.spark.domain.{FsConfig, LocalFsConfig, S3FsConfig}
import rever.rsparkflow.spark.utils.PathExtension.PathExtension
import rever.rsparkflow.spark.utils.{PathBuilder, PathExtension, Utils}

import java.io.InputStream

/**
  * @author anhlt - aka andy
**/

object FsClient {
  private val clientMap = scala.collection.mutable.Map.empty[FsConfig, FsClient]

//  def s3Client(config: S3FsConfig): FsClient = {
//    client(config)
//  }

  def client(config: FsConfig): FsClient = {
    config match {
      case config: S3FsConfig =>
        createClient(config) {
          S3FsClient(config)
        }
      case config: LocalFsConfig =>
        createClient(config) {
          LocalFsClient(config)
        }

      case config => throw new Exception(s"Unknown file system: ${config}")
    }
  }

  private def createClient(config: FsConfig)(fn: => FsClient): FsClient = {
    clientMap.get(config) match {
      case Some(client) => client
      case None =>
        clientMap.synchronized {
          clientMap
            .get(config)
            .fold[FsClient]({
              val client = fn
              clientMap.put(config, client)
              client
            })(x => x)

        }
    }
  }

}

trait FsClient extends Serializable {
  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)


  def pathBuilder(extension: PathExtension = PathExtension.PARQUET): PathBuilder

  def sparkFilePath(paths: String*): String

  def estimateSizeInBytes(bucketKey: String): Option[Long]

  def listObjects(bucketKey: Option[String] = None): Seq[String]

  def writeObject(
      objectId: String,
      is: InputStream,
      contentLength: Option[Long] = None,
      contentType: Option[String] = None,
      metadata: Option[Map[String, String]] = None
  ): WriteObjectResult

  def existBucket(bucketKey: String): Boolean

  def existObject(objectKey: String): Boolean

  final def estimateNumOfPartitions(
      bucketKey: String,
      partitionSizeInBytes: Long,
      defaultPartitionNum: Int = 10
  ): Int = {
    val fileSize = estimateSizeInBytes(bucketKey)

    fileSize.fold(defaultPartitionNum)(Utils.estimateNoOfPartitions(_, partitionSizeInBytes))
  }
}
