//package reader
//
//import org.apache.spark.sql.SparkSession
//import rever.etl.rsparkflow.api.configuration.{Config, DefaultConfig}
//import rever.etl.rsparkflow.domain.S3Config
//import rever.etl.rsparkflow.utils.Utils
//
//import scala.collection.JavaConverters.mapAsJavaMapConverter
//
//object OppoByPhaseReaderTest {
//  def main(args: Array[String]): Unit = {
//    SparkSession
//      .builder()
//      .master("local[2]")
//      .getOrCreate()
//
//    val configuration: Config = new DefaultConfig(
//      Map[String, AnyRef](
//        "RV_S3_ACCESS_KEY" -> "AKIAWKIXPEEAN6S4A45C",
//        "RV_S3_SECRET_KEY" -> "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",
//        "RV_S3_REGION" -> "ap-southeast-1",
//        "RV_S3_BUCKET" -> "rever-analysis",
//        "RV_S3_PARENT_PATH" -> "temp",
//        "a0Path" -> "A0/oppo_by_phase/2019/12/18_12_2019.parquet",
//        "a1Path" -> "A1/oppo_by_phase/2019/12/18_12_2019.parquet"
//      ).asJava
//    )
//    Utils.applyS3Config(SparkSession.active, S3Config(configuration))
//    readFile(configuration, configuration.get("a0Path"))
//    readFile(configuration, configuration.get("a1Path"))
//
//  }
////  def readFile(config: Config, path: String): Unit = {
////    val s3Config = S3Config(config)
////    val s3Path = s"s3a://${s3Config.bucket}/${s3Config.parentPath}/$path"
////    SparkSession.active.read
////      .parquet(s3Path)
////      .show(10)
////  }
//
//}
