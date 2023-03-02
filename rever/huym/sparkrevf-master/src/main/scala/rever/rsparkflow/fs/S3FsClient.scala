package rever.rsparkflow.fs

import rever.rsparkflow.spark.domain.S3FsConfig
import rever.rsparkflow.spark.utils.PathExtension.PathExtension
import rever.rsparkflow.spark.utils.{PathBuilder, PathExtension}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

import java.io.InputStream
import scala.jdk.CollectionConverters.{asScalaIteratorConverter, mapAsJavaMapConverter}

/**
 * @author anhlt - aka andy
 **/
case class S3FsClient(config: S3FsConfig) extends FsClient {

  final val client = S3Client
    .builder()
    .credentialsProvider(
      StaticCredentialsProvider.create(AwsBasicCredentials.create(config.accessKey, config.secretKey))
    )
    .region(Region.of(config.region))
    .build()

  def pathBuilder(extension: PathExtension = PathExtension.PARQUET): PathBuilder = {
    PathBuilder.build(config.parentPath, extension)
  }

  override def sparkFilePath(paths: String*): String = {
    Seq(Seq("s3a:/", config.bucket), paths.toSeq).flatten
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .mkString("/")
  }

  override def estimateSizeInBytes(bucketKey: String): Option[Long] = {
    val estimateSizeInBytes = getObjects(Some(bucketKey))
      .foldLeft(0L)((size, s3Object) => s3Object.data.size() + size)

    Some(estimateSizeInBytes)
  }

  override def existBucket(bucketKey: String): Boolean = {
    try {
      val builder = ListObjectsRequest
        .builder()
        .bucket(config.bucket)
        .prefix(bucketKey)
        .maxKeys(1)

      val response = client.listObjects(builder.build())

      response.contents() != null && !response.contents().isEmpty
    } catch {
      case ex: NoSuchKeyException => false
    }
  }

  override def existObject(objectKey: String): Boolean = {
    try {
      val builder = GetObjectRequest
        .builder()
        .bucket(config.bucket)
        .key(objectKey)
      val response = client.getObject(builder.build())

      response.response() != null
    } catch {
      case ex: NoSuchKeyException => false
    }
  }

  override def listObjects(bucketKey: Option[String] = None): Seq[String] = {
    getObjects(bucketKey).map(_.data.key())
  }

  private def getObjects(bucketKey: Option[String] = None): Seq[S3File] = {

    val builder = ListObjectsV2Request
      .builder()
      .bucket(config.bucket)

    bucketKey.foreach(builder.prefix)

    val response = client.listObjectsV2Paginator(builder.build())

    response
      .contents()
      .iterator()
      .asScala
      .toSeq
      .map(f => S3File(f))
  }

  override def writeObject(
      objectId: String,
      is: InputStream,
      contentLength: Option[Long] = None,
      contentType: Option[String] = None,
      metadata: Option[Map[String, String]] = None
  ): WriteObjectResult = {
    val builder = PutObjectRequest
      .builder()
      .bucket(config.bucket)
      .key(objectId)
      .contentType(contentType.getOrElse("application/octet-stream"))

    metadata.foreach(entry => builder.metadata(entry.asJava))

    val response = client.putObject(
      builder.build(),
      RequestBody.fromInputStream(is, is.available())
    )

    WriteObjectResult(etag = response.eTag(), "", objectId = objectId)
  }

}
