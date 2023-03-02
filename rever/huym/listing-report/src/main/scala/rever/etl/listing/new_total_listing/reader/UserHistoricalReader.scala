package rever.etl.listing.new_total_listing.reader

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.utils.TimestampUtils

import scala.concurrent.duration.DurationInt

class UserHistoricalReader extends SourceReader with FlowMixin {

  override def read(s: String, config: Config): Dataset[Row] = {
    val reportTime = config.getDailyReportTime

    val userHistoricalJobId = config.get("user_historical_job_id", "user_historical")

    val schema = StructType(
      Seq(
        StructField("username", DataTypes.StringType, nullable = false),
        StructField("user_type", DataTypes.StringType, nullable = false),
        StructField("business_unit", DataTypes.StringType, nullable = false),
        StructField("market_center_id", DataTypes.StringType, nullable = false),
        StructField("team_id", DataTypes.StringType, nullable = false),
        StructField("status", DataTypes.StringType, nullable = false)
      )
    )
    getPreviousA0DfFromS3(userHistoricalJobId, reportTime + 1.days.toMillis, config, schema = Some(schema)).getOrElse(
      throw new Exception(s"No User A0 file was found for date: ${TimestampUtils.format(reportTime)}")
//      SparkSession.active.emptyDataset(RowEncoder(schema))
    )

  }

}
