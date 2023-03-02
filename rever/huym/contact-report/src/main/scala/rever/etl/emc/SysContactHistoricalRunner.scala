package rever.etl.emc

import org.apache.spark.sql.SparkSession
import rever.etl.rsparkflow.BaseRunner
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.api.udf.RUdfUtils
import rever.etl.rsparkflow.domain.SparkSessionImplicits.SparkSessionImplicits
import rever.etl.rsparkflow.utils.Utils

import scala.collection.JavaConverters.mapAsJavaMapConverter

object SysContactHistoricalRunner extends BaseRunner {
  def main(args: Array[String]): Unit = {
    val config = Config
      .builder()
      .addClickHouseArguments()
      .addS3Arguments()
      .addForceRunArguments()
      .build(Utils.parseArgToMap(args).asJava, true)

    SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    SparkSession.active.sparkContext.setLogLevel("ERROR")
    SparkSession.active.initS3(config)
    RUdfUtils.registerAll(SparkSession.active)

    runWithDailyForceRun("rever.etl.emc.jobs.historical.sys_contact", config)

  }

}
