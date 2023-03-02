package rever.etl.emc

import org.apache.spark.sql.SparkSession
import rever.etl.rsparkflow.BaseRunner
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.api.udf.RUdfUtils
import rever.etl.rsparkflow.utils.Utils

import org.apache.spark.sql.SparkSession
import rever.etl.rsparkflow.BaseRunner
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.api.udf.RUdfUtils
import rever.etl.rsparkflow.domain.SparkSessionImplicits.SparkSessionImplicits
import rever.etl.rsparkflow.utils.Utils

import java.time.ZoneId
import java.util.TimeZone
import scala.jdk.CollectionConverters.mapAsJavaMapConverter

object HotLeadRunner extends BaseRunner {
  def main(args: Array[String]): Unit = {
    val config = Config
      .builder()
      .addClickHouseArguments()
      .addS3Arguments()
      .addForceRunArguments()
      .addArgument("system_tags", true)
      .build(Utils.parseArgToMap(args).asJava, true)

    SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    SparkSession.active.sparkContext.setLogLevel("ERROR")
    SparkSession.active.initS3(config)
    RUdfUtils.registerAll(SparkSession.active)

    runWithDailyForceRun("rever.etl.emc.tool.hot_lead_export", config)
  }

}
