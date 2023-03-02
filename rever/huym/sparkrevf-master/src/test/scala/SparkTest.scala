import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Column, RowFactory, SparkSession}
import org.scalatest.FunSuite
import rever.rsparkflow.spark.api.udf.RUdfUtils

import java.util

/**
  * @author anhlt
  */
class SparkTest extends FunSuite {

  private val sparkSession = SparkSession
    .builder()
    .master("local[2]")
    .getOrCreate()

  RUdfUtils.registerAll(sparkSession)

//  test("Test") {
//
//    val config = Config
//      .builder()
//      .addArgument("RV_S3_ACCESS_KEY", true)
//      .addArgument("RV_S3_SECRET_KEY", true)
//      .addArgument("RV_S3_REGION", true)
//      .addArgument("RV_S3_BUCKET", true)
//      .addArgument("RV_S3_PARENT_PATH", true)
//      .addArgument("RV_RAP_INGESTION_HOST", true)
//      .addArgument("input_path", true)
//      .addArgument("output_path", true)
//      .build(
//        Map[String, AnyRef](
//          "RV_EXECUTION_DATE" -> "2022-01-01T12:00:00+00:00",
//          "RV_JOB_ID" -> "rever_etl_example",
//          "RV_SPARK_JOB_PACKAGE" -> "rever.etl.example",
//          "RV_S3_ACCESS_KEY" -> "AKIAWKIXPEEAL3S75TBU",
//          "RV_S3_SECRET_KEY" -> "pXCVbpuvwIWTwwnYR5YVJzHe6CbACKMBEJwKccvh",
//          "RV_S3_REGION" -> "ap-southeast-1",
//          "RV_S3_BUCKET" -> "rever-analysis-staging",
//          "RV_S3_PARENT_PATH" -> "temp",
//          "RV_RAP_INGESTION_HOST" -> "http://eks-svc.reverland.com:31482",
//          "input_path" -> "./src/test/resources/test-data",
//          "output_path" -> ".tmp/output"
//        ).asJava,
//        true
//      )
//
//    val sparkSession = SparkSession
//      .builder()
//      .master("local[2]")
//      .getOrCreate()
//
//    S3Config(config).applyFor(sparkSession)
//
//    val df = sparkSession.read.parquet(
//      "s3a://rever-analysis/A1/isa_report_oppo_by_phase/2022/01/01_01_2022.parquet",
//      "s3a://rever-analysis/A1/isa_report_oppo_by_phase/2022/01/02_01_2022.parquet",
//      "s3a://rever-analysis/A1/isa_report_oppo_by_phase/2022/01/03_01_2022.parquet"
//    )
//
//    df.printSchema()
//    df.show(100)
//
//  }

  test("Case When ") {

    val df = sparkSession
      .createDataFrame(
        util.Arrays.asList(
          RowFactory.create("1", "A", "Test only"),
          RowFactory.create(
            "2",
            "B",
            null
          ),
          RowFactory.create(
            "3",
            "C",
            null
          )
        ),
        StructType(
          Array(
            StructField("id", DataTypes.StringType, false),
            StructField("name", DataTypes.StringType, false),
            StructField("desc", DataTypes.StringType, true)
          )
        )
      )
      .withColumn("desc", when(col("desc").isNull, lit("unknown")).otherwise(col("desc")))

    df.printSchema()
    df.show(10, truncate = 100)

  }

  test("UDF: Parse User Agent") {

    val df = sparkSession
      .createDataFrame(
        util.Arrays.asList(
          RowFactory.create("1", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:100.0) Gecko/20100101 Firefox/100.0"),
          RowFactory.create(
            "2",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36"
          ),
          RowFactory.create(
            "3",
            "Mozilla/5.0 (Linux; Android 12; SM-S906N Build/QP1A.190711.020; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/80.0.3987.119 Mobile Safari/537.36"
          )
        ),
        StructType(
          Array(
            StructField("id", DataTypes.StringType, false),
            StructField("user_agent", DataTypes.StringType, false)
          )
        )
      )
      .withColumn("client_info", callUDF(RUdfUtils.RV_PARSE_USER_AGENT, col("user_agent")))
      .drop(col("user_agent"))

    val finalDf = df.select(flattenStructSchema(df.schema): _*)

    finalDf.printSchema()

    finalDf.show(10, truncate = 100)

  }

  def flattenStructSchema(schema: StructType, prefix: Option[String] = None): Array[Column] = {
    schema.fields.flatMap(f => {
      val columnName = if (prefix.isEmpty) f.name else (prefix.getOrElse("") + "." + f.name)

      f.dataType match {
        case st: StructType => flattenStructSchema(st, Option(columnName))
        case _              => Array(col(columnName).as(columnName.replace(".", "_")))
      }
    })
  }

}
