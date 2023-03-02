package rever.rsparkflow.spark.domain

/**
  * @author anhlt - aka andy
 **/
trait FsConfig {
  val parentPath: String
}

case class S3FsConfig(
    accessKey: String,
    secretKey: String,
    region: String,
    bucket: String,
    parentPath: String
) extends FsConfig

case class LocalFsConfig(
    parentPath: String
) extends FsConfig
