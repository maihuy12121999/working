import org.scalatest.FunSuite
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.utils.{JsonUtils, TimestampUtils, Utils}

import scala.collection.JavaConverters.mapAsJavaMapConverter

/**
  * @author anhlt
  */
class UtilTest extends FunSuite {

  test("Config get execution date info") {
    val config = Config
      .builder()
      .addArgument("RV_S3_ACCESS_KEY", true)
      .addArgument("RV_S3_SECRET_KEY", true)
      .addArgument("RV_S3_REGION", true)
      .addArgument("RV_S3_BUCKET", true)
      .addArgument("RV_S3_PARENT_PATH", true)
      .addArgument("RV_RAP_INGESTION_HOST", true)
      .addArgument("input_path", true)
      .addArgument("output_path", true)
      .build(
        Map[String, AnyRef](
          "RV_PREV_EXECUTION_DATE" -> "2022-01-01T12:00:00+00:00",
          "RV_EXECUTION_DATE" -> "2022-01-01T12:00:00+00:00",
          "RV_JOB_ID" -> "rever_etl_example",
          "RV_SPARK_JOB_PACKAGE" -> "rever.etl.example",
          "RV_S3_ACCESS_KEY" -> "AKIAWKIXPEEAL3S75TBU",
          "RV_S3_SECRET_KEY" -> "mpXCVbpuvwIWTwwnYR5YVJzHe6CbACKMBEJwKccvh",
          "RV_S3_REGION" -> "ap-southeast-1",
          "RV_S3_BUCKET" -> "rever-analysis-staging",
          "RV_S3_PARENT_PATH" -> "temp",
          "RV_RAP_INGESTION_HOST" -> "http://eks-svc.reverland.com:31482",
          "input_path" -> "./src/test/resources/test-data",
          "output_path" -> ".tmp/output"
        ).asJava,
        true
      )

    assertResult(Some(1641038400000L))(config.getExecutionDateInfo.prevExecutionTime)
    assertResult(1641038400000L)(config.getExecutionDateInfo.executionTime)
  }

  test("Config execution time") {
    val config = Config
      .builder()
      .addArgument("RV_S3_ACCESS_KEY", true)
      .addArgument("RV_S3_SECRET_KEY", true)
      .addArgument("RV_S3_REGION", true)
      .addArgument("RV_S3_BUCKET", true)
      .addArgument("RV_S3_PARENT_PATH", true)
      .addArgument("RV_RAP_INGESTION_HOST", true)
      .addArgument("input_path", true)
      .addArgument("output_path", true)
      .build(
        Map[String, AnyRef](
          "RV_PREV_EXECUTION_DATE" -> "2022-01-01T12:00:00+00:00",
          "RV_EXECUTION_DATE" -> "2022-01-01T12:00:00+00:00",
          "RV_JOB_ID" -> "rever_etl_example",
          "RV_SPARK_JOB_PACKAGE" -> "rever.etl.example",
          "RV_S3_ACCESS_KEY" -> "AKIAWKIXPEEAL3S75TBU",
          "RV_S3_SECRET_KEY" -> "pXCVbpuvwIWTwwnYR5YVJzHe6CbACKMBEJwKccvh",
          "RV_S3_REGION" -> "ap-southeast-1",
          "RV_S3_BUCKET" -> "rever-analysis-staging",
          "RV_S3_PARENT_PATH" -> "temp",
          "RV_RAP_INGESTION_HOST" -> "http://eks-svc.reverland.com:31482",
          "input_path" -> "./src/test/resources/test-data",
          "output_path" -> ".tmp/output"
        ).asJava,
        true
      )

    assertResult(true)(config.getPrevExecutionTime.isPresent)
    assertResult(1641038400000L)(config.getPrevExecutionTime.get)
    assertResult(1641038400000L)(config.getExecutionTime)
  }

  test("Config Prev execution time") {
    val config = Config
      .builder()
      .addArgument("RV_S3_ACCESS_KEY", true)
      .addArgument("RV_S3_SECRET_KEY", true)
      .addArgument("RV_S3_REGION", true)
      .addArgument("RV_S3_BUCKET", true)
      .addArgument("RV_S3_PARENT_PATH", true)
      .addArgument("RV_RAP_INGESTION_HOST", true)
      .addArgument("input_path", true)
      .addArgument("output_path", true)
      .build(
        Map[String, AnyRef](
          "RV_EXECUTION_DATE" -> "2022-01-01T12:00:00+00:00",
          "RV_JOB_ID" -> "rever_etl_example",
          "RV_SPARK_JOB_PACKAGE" -> "rever.etl.example",
          "RV_S3_ACCESS_KEY" -> "AKIAWKIXPEEAL3S75TBU",
          "RV_S3_SECRET_KEY" -> "pXCVbpuvwIWTwwnYR5YVJzHe6CbACKMBEJwKccvh",
          "RV_S3_REGION" -> "ap-southeast-1",
          "RV_S3_BUCKET" -> "rever-analysis-staging",
          "RV_S3_PARENT_PATH" -> "temp",
          "RV_RAP_INGESTION_HOST" -> "http://eks-svc.reverland.com:31482",
          "input_path" -> "./src/test/resources/test-data",
          "output_path" -> ".tmp/output"
        ).asJava,
        true
      )

    assertResult(false)(config.getPrevExecutionTime.isPresent)
  }

  test("Test run With Daily Cacheup") {
    val startReportTime = TimestampUtils.parseMillsFromString("2022-01-01T00:00:00+07:00", "yyyy-MM-dd'T'HH:mm:ssXXX")

    val endReportTime = TimestampUtils.parseMillsFromString("2022-01-01T00:00:00+07:00", "yyyy-MM-dd'T'HH:mm:ssXXX")

    for (reportTime <- startReportTime to (endReportTime, 1.days.toMillis)) {
      println(TimestampUtils.format(reportTime, Some("yyyy-MM-dd'T'HH:mm:ssXXX")))
      println("----------------------------------------------------------------------")
    }
  }

  test("Parse execution time") {
    val s = "2022-06-02T07:14:20.276082+00:00"

    val time = TimestampUtils.parseMillsFromString(s, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")

    println(s"Time: ${time}")

    val patterns = Seq(
      "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX",
      "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
      "yyyy-MM-dd'T'HH:mm:ssXXX"
    )

    val times = patterns.flatMap(pattern => {
      try {
        Some(TimestampUtils.parseMillsFromString(s, pattern))
      } catch {
        case _ => None
      }
    })

    times.headOption.get

  }

  test("Get all quarters") {
    val time = TimestampUtils.asStartOfYear(System.currentTimeMillis())

    val quarters = TimestampUtils.getAllQuarters(time)

    quarters.foreach(time => {
      println(TimestampUtils.format(time))
    })
  }

  test("Get all months") {
    val time = TimestampUtils.asStartOfYear(System.currentTimeMillis())

    val quarters = TimestampUtils.getAllMonths(time)

    quarters.foreach(time => {
      println(TimestampUtils.format(time))
    })

    println("---------")
  }

  test("Get all weeks") {
    val time = TimestampUtils.asStartOfYear(System.currentTimeMillis())

    val quarters = TimestampUtils.getAllWeeks(time)

    quarters.foreach(time => {
      println(TimestampUtils.format(time))
    })
    println("---------")
  }

  test("Get all days") {
    val time = TimestampUtils.asStartOfYear(System.currentTimeMillis())

    val quarters = TimestampUtils.getAllDays(time)

    quarters.foreach(time => {
      println(TimestampUtils.format(time))
    })
    println("---------")
  }

  test("Test durations") {
    val begin = System.currentTimeMillis()
    Thread.sleep(5000)
    val d = System.currentTimeMillis() - begin

    println(TimestampUtils.durationAsText(d * 1000))
  }

  test("Test config: spark configs") {
    val sparkConfigStr = JsonUtils.toJson(
      Map(
        "spark.sql.adaptive.enabled" -> "true",
        "spark.sql.adaptive.coalescePartitions.enabled" -> "true"
      )
    )

    val config = Config
      .builder()
      .build(
        Map[String, AnyRef](
          "RV_JOB_ID" -> "abc",
          "RV_EXECUTION_DATE" -> "2020-08-11T08:12:12+00:00",
          "RV_SPARK_CONFIGS" -> sparkConfigStr
        ).asJava,
        true
      )

    val sparkConfig = config.getSparkConfigMap

    println(sparkConfig)
    assertResult(false)(sparkConfig.isEmpty)
    assertResult("true")(sparkConfig.get("spark.sql.adaptive.enabled"))
    assertResult("true")(sparkConfig.get("spark.sql.adaptive.coalescePartitions.enabled"))
  }

  test("Test config: spark config 2") {
    val args = Array(
      "--conf",
      "spark.sql.adaptive.enabled=true",
      "--conf",
      "spark.sql.adaptive.coalescePartitions.enabled=true"
    )

    val config = Utils.parseArgToMap(args)

    println(config)

  }

  test("Test parse args") {

    val args = Array(
      "---report_date",
      "2022-04-27 00:00:00+00:00",
      "---CH_DRIVER",
      "ru.yandex.clickhouse.ClickHouseDriver",
      "--RV_RAP_INGESTION_HOST",
      "--RV_RAP_INGESTION_HOST2"
    )

    val map = Utils.parseArgToMap(args)

    println(map)

    assertResult(true)(map.contains("report_date"))
    assertResult(true)(map.contains("CH_DRIVER"))
    assertResult(false)(map.contains("RV_RAP_INGESTION_HOST"))
  }

  test("Parse execution date") {
    val time = TimestampUtils.parseMillsFromString("2022-04-27T19:00:00+00:00", "yyyy-MM-dd'T'HH:mm:ssXXX")

    println(time)

    println(TimestampUtils.format(time))
  }

  test("Parse execution date 2 ") {
    val time = TimestampUtils.parseMillsFromString("2021-05-13T19:00:00+00:00", "yyyy-MM-dd'T'HH:mm:ssXXX")

    println(time)
    println(TimestampUtils.format(time))
  }

  test("Test castersion product") {
    val columns = List(
      List("1"),
      List("1020", "all"),
      List("male", "all"),
      List("active", "all"),
      List("1000000")
    )

    val rows = Utils.cartesianProduct(columns)

    rows.foreach(row => println(row))
  }
  test("Is end of quarter") {
    assertResult(true)(TimestampUtils.isEndOfQuarter(TimestampUtils.parseMillsFromString("2022-03-31", "yyyy-MM-dd")))
    assertResult(true)(TimestampUtils.isEndOfQuarter(TimestampUtils.parseMillsFromString("2022-06-30", "yyyy-MM-dd")))
    assertResult(false)(TimestampUtils.isEndOfQuarter(TimestampUtils.parseMillsFromString("2022-07-30", "yyyy-MM-dd")))
    assertResult(false)(TimestampUtils.isEndOfQuarter(TimestampUtils.parseMillsFromString("2022-10-30", "yyyy-MM-dd")))
  }

  test("Is end of month") {
    assertResult(false)(TimestampUtils.isEndOfMonth(TimestampUtils.parseMillsFromString("2022-01-25", "yyyy-MM-dd")))
    assertResult(true)(TimestampUtils.isEndOfMonth(TimestampUtils.parseMillsFromString("2022-01-31", "yyyy-MM-dd")))
    assertResult(true)(TimestampUtils.isEndOfMonth(TimestampUtils.parseMillsFromString("2022-02-28", "yyyy-MM-dd")))
    assertResult(false)(TimestampUtils.isEndOfMonth(TimestampUtils.parseMillsFromString("2022-02-01", "yyyy-MM-dd")))
  }

  test("Is end of week") {
    assertResult(true)(TimestampUtils.isEndOfWeek(TimestampUtils.parseMillsFromString("2022-09-04", "yyyy-MM-dd")))
    assertResult(true)(TimestampUtils.isEndOfWeek(TimestampUtils.parseMillsFromString("2022-09-11", "yyyy-MM-dd")))
    assertResult(true)(TimestampUtils.isEndOfWeek(TimestampUtils.parseMillsFromString("2022-09-25", "yyyy-MM-dd")))
    assertResult(false)(TimestampUtils.isEndOfWeek(TimestampUtils.parseMillsFromString("2022-09-26", "yyyy-MM-dd")))
  }

  test("Class should be in the given package name") {
    val packageName = "rever.etl.example"

    assertResult(true)(Utils.isBelongToPackage(packageName, "rever.etl.example.reader.ABCReader"))
    assertResult(false)(Utils.isBelongToPackage(packageName, "rever.etl.example_example.reader.ABCReader"))
  }
}
