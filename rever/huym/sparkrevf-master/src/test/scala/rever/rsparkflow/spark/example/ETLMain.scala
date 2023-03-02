package rever.rsparkflow.spark.example

import org.apache.spark.sql.SparkSession
import rever.rsparkflow.implicits.SparkSessionImplicits.SparkSessionImplicits
import rever.rsparkflow.spark.RSparkFlow
import rever.rsparkflow.spark.api.configuration.Config

import scala.jdk.CollectionConverters.mapAsJavaMapConverter

/**
  * @author anhlt
  */
object ETLMain {

  def main(args: Array[String]): Unit = {

    val config = Config
      .builder()
      .addRapIngestionClientArguments()
      .addArgument("input_path", true)
      .addArgument("output_path", true)
      .build(
        Map[String, AnyRef](
          "RV_JOB_ID" -> "rever_rsparkflow_example",
          "RV_EXECUTION_DATE" -> "2023-01-01",
//          "RV_S3_ACCESS_KEY" -> "AKIAWKIXPEEAL3S75TBU",
//          "RV_S3_SECRET_KEY" -> "pXCVbpuvwIWTwwnYR5YVJzHe6CbACKMBEJwKccvh",
//          "RV_S3_REGION" -> "ap-southeast-1",
//          "RV_S3_BUCKET" -> "rever-analysis-staging",
          "RV_FS_PARENT_PATH" -> "./data",
          "RV_RAP_INGESTION_HOST" -> "http://eks-svc.reverland.com:31482",
          "input_path" -> "./src/test/resources/test-data",
          "output_path" -> ".tmp/output"
        ).asJava,
        true
      )

    val sparkSession = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    sparkSession.initS3(config)

    new RSparkFlow().run("rever.rsparkflow.spark.example", config)

  }

}
