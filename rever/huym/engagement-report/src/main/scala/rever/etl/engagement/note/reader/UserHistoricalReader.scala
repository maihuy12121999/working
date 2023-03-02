package rever.etl.engagement.note.reader

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.engagement.domain.UserTypes
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.utils.TimestampUtils

import scala.concurrent.duration.DurationInt

class UserHistoricalReader extends SourceReader with FlowMixin {

  override def read(s: String, config: Config): Dataset[Row] = {
    val reportTime = config.getDailyReportTime

    val userHistoricalJobId = config.get("user_historical_job_id", "user_historical")
    val rvaJobTitles = config.get("rva_job_titles").split(",")
    val smJobTitles = config.get("sm_job_titles").split(",")
    val sdJobTitles = config.get("sd_job_titles").split(",")
    val lcJobTitles = config.get("lc_job_titles").split(",")

    val schema = StructType(
      Seq(
        StructField("username", DataTypes.StringType, nullable = false),
        StructField("user_type", DataTypes.StringType, nullable = false),
        StructField("business_unit", DataTypes.StringType, nullable = false),
        StructField("market_center_id", DataTypes.StringType, nullable = false),
        StructField("team_id", DataTypes.StringType, nullable = false),
        StructField("status", DataTypes.StringType, nullable = false),
        StructField("job_title", DataTypes.StringType, nullable = false)
      )
    )
    getPreviousA0DfFromS3(userHistoricalJobId, reportTime + 1.days.toMillis, config, schema = Some(schema)).getOrElse(
      throw new Exception(s"No User A0 file was found for date: ${TimestampUtils.format(reportTime)}")
//      SparkSession.active.emptyDataset(RowEncoder(schema))
    )
      .withColumn("user_type",
        when(col("job_title").isin(rvaJobTitles:_*),UserTypes.RVA)
          .when(col("job_title").isin(smJobTitles:_*),UserTypes.SM)
          .when(col("job_title").isin(sdJobTitles:_*),UserTypes.SD)
          .when(col("job_title").isin(lcJobTitles:_*),UserTypes.LC)
          .otherwise(UserTypes.BO)
      )
  }

}
