package rever.etl.call.jobs.non_call_users.reader

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.utils.TimestampUtils

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class UserHistoricalReader extends SourceReader with FlowMixin{
  override def read(tableName: String, config: Config): Dataset[Row] = {
    val reportTime = config.getDailyReportTime
    val userHistoricalJobId = config.get("user_historical_job_id","user_historical")
    val rvaJobTitles = config.get("rva_job_titles").split(",")
    val smJobTitles = config.get("sm_job_titles").split(",")
    val sdJobTitles = config.get("sd_job_titles").split(",")
    val jobTitleGroups = rvaJobTitles++smJobTitles++sdJobTitles
    val schema = StructType(
      Seq(
        StructField("username",DataTypes.StringType,nullable = false),
        StructField("user_type",DataTypes.StringType,nullable = false),
        StructField("status",DataTypes.StringType,nullable = false),
        StructField("market_center_id",DataTypes.StringType,nullable = false),
        StructField("team_id",DataTypes.StringType,nullable = false),
        StructField("job_title",DataTypes.StringType,nullable = false)
      )
    )
    getPreviousA0DfFromS3(userHistoricalJobId,reportTime+1.days.toMillis,config,schema = Some(schema)).getOrElse(
      throw new Exception(s"No User A0 file was found for date: ${TimestampUtils.format(reportTime)}")
//      SparkSession.active.emptyDataset(RowEncoder(schema))
//      SparkSession.active
//        .read
//        .format("csv")
//        .option("header","true")
//        .load("data/A0user.csv")
    )
      .where(
        col("status")==="1" && col("job_title").isin(jobTitleGroups:_*)
      )
  }
}
