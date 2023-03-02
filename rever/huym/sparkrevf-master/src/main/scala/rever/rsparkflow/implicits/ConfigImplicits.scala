package rever.rsparkflow.implicits

import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.domain.{ClickHouseConfig, FsConfig, LocalFsConfig, S3FsConfig}
import rever.rsparkflow.spark.utils.JsonUtils

import java.util

object ConfigImplicits {

  implicit class ConfigImplicit(val config: Config) extends AnyVal {

    def clickHouseConfig: ClickHouseConfig = {
      val driver = config.get("CH_DRIVER")
      val host = config.get("CH_HOST")
      val port = config.getInt("CH_PORT")
      val userName = config.get("CH_USER_NAME")
      val password = config.get("CH_PASSWORD")
      val dbName = config.get("CH_DB", "")
      val tableName = config.get("CH_TABLE", "")
      val properties = JsonUtils.fromJson[Map[String, Any]](config.get("CH_DATASOURCE_CONFIG", "{}"))
      ClickHouseConfig(
        driver,
        host,
        port,
        userName,
        password,
        Option(dbName).filterNot(_ == null).filterNot(_.isEmpty),
        Option(tableName).filterNot(_ == null).filterNot(_.isEmpty),
        Some(properties)
      )
    }

    def fsConfig: FsConfig = {
      val fs = config.get("RV_FS", "local").toLowerCase

      val (s3FsConfig, localFsConfig) = (optS3Config, optLocalFsConfig)

      (s3FsConfig, fs) match {
        case (Some(fsConfig: S3FsConfig), _: String) => fsConfig
        case _                                       => localFsConfig.getOrElse(LocalFsConfig(""))
      }

    }

    def s3Config: S3FsConfig = {
      optS3Config.getOrElse(throw new Exception("No S3 config."))
    }

    def optS3Config: Option[S3FsConfig] = {

      val hasFsConfig = config.hasOneOf(util.Arrays.asList("RV_S3_ACCESS_KEY", "RV_S3_SECRET_KEY", "RV_S3_BUCKET"))

      if (hasFsConfig) {
        val accessKey = config.get("RV_S3_ACCESS_KEY")
        val secretKey = config.get("RV_S3_SECRET_KEY")
        val region = config.get("RV_S3_REGION")
        val bucket = config.get("RV_S3_BUCKET")
        val parentPath = Seq(
          config.get("RV_FS_PARENT_PATH", ""),
          config.get("RV_S3_PARENT_PATH", "")
        ).head

        Some(S3FsConfig(accessKey, secretKey, region, bucket, parentPath))
      } else {
        None
      }
    }

    def optLocalFsConfig: Option[LocalFsConfig] = {

      val hasFsConfig = config.hasOneOf(util.Arrays.asList("RV_FS_PARENT_PATH", "RV_S3_PARENT_PATH"))

      if (hasFsConfig) {
        val parentPath = Seq(
          config.get("RV_FS_PARENT_PATH", ""),
          config.get("RV_S3_PARENT_PATH", "")
        ).head

        Some(LocalFsConfig(parentPath))
      } else {
        None
      }
    }

  }
}
