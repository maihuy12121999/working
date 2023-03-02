package rever.etl.support.cs2_call_cta_analysis

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import rever.rsparkflow.spark.FlowMixin
import rever.rsparkflow.spark.api.annotation.{Output, Table}
import rever.rsparkflow.spark.api.configuration.Config
import rever.etl.support.cs2_call_cta_analysis.reader.{Call3cxExtensionReader, CallHistoryReader}
import rever.etl.support.cs2_call_cta_analysis.writer.CallCtaAnalysisWriter

class Cs2CallCtaAnalysis extends FlowMixin {

  @Output(writer = classOf[CallCtaAnalysisWriter])
  @Table("call_cta_analysis")
  def build(
      @Table("call_cta_events") callCtaEventDf: DataFrame,
      @Table(name = "call_histories", reader = classOf[CallHistoryReader]) callHistoryDf: DataFrame,
      @Table(
        name = "call_extension",
        reader = classOf[Call3cxExtensionReader]
      ) callExtensionDf: DataFrame,
      config: Config
  ): DataFrame = {
    val callHistoryBroadcast = SparkSession.active.sparkContext.broadcast(callHistoryDf.sort("timestamp").collect())

    val finalDf = callCtaEventDf
      .map(matchCtaAndCallRow(_, callHistoryBroadcast, CallCtaHelper.callCtaAnalysisSchema, config))(
        RowEncoder(CallCtaHelper.callCtaAnalysisSchema)
      )
      .toDF(CallCtaHelper.callCtaAnalysisSchema.fields.map(_.name): _*)

    enhanceRealPhoneFromExt(finalDf, callExtensionDf)
  }

  def matchCtaAndCallRow(
      ctaRow: Row,
      callHistoryBroadcast: Broadcast[Array[Row]],
      schema: StructType,
      config: Config
  ): Row = {

    def isMatch(userPhoneNumber: String, callerPhone: String, clickedAt: Long, callAt: Long): Boolean = {
      val maxDurationAfterClicked = config.getLong("max_duration_after_clicked")
      val maxDurationAfterUserClicked = config.getLong("max_duration_after_user_clicked")

      userPhoneNumber.equalsIgnoreCase(callerPhone) match {
        case true if callAt >= clickedAt && callAt <= (clickedAt + maxDurationAfterUserClicked) => true
        case false if callAt >= clickedAt && callAt <= (clickedAt + maxDurationAfterClicked)    => true
        case _                                                                                  => false
      }
    }

    val clickedAt = ctaRow.getAs[Long]("timestamp")
    val messageId = ctaRow.getAs[String]("message_id")
    val anonymousId = ctaRow.getAs[String]("anonymous_id")
    val userId = ctaRow.getAs[String]("user_id")
    val url = ctaRow.getAs[String]("url")
    val userPhoneNumber = ctaRow.getAs[String]("user_phone_number")
    val sourceType = ctaRow.getAs[String]("source_type")
    val sourceCode = ctaRow.getAs[String]("source_code")
    val event = ctaRow.getAs[String]("event")
    val os = ctaRow.getAs[String]("os")
    val platform = ctaRow.getAs[String]("platform")

    val callFromCtaRows = callHistoryBroadcast.value
      .filter { callRow =>
        val callerPhone = callRow.getAs[String]("call_from") match {
          case phone if phone.startsWith("+") => phone.substring(1)
          case phone                          => phone
        }
        val callStartTime = callRow.getAs[Long]("timestamp")

        isMatch(userPhoneNumber, callerPhone, clickedAt, callStartTime)
      }
      .sortBy(row => {
        val callerPhone = row.getAs[String]("call_from") match {
          case phone if phone.startsWith("+") => phone.substring(1)
          case phone                          => phone
        }
        val callStartTime = row.getAs[Long]("timestamp")

        (callerPhone != userPhoneNumber, callStartTime)
      })

    callFromCtaRows.headOption match {
      case Some(row) =>
        val callId = row.getAs[String]("call_id")
        val callerPhone = row.getAs[String]("call_from")
        val receiverPhone = row.getAs[String]("call_to")
        val extension = row.getAs[String]("extension")
        val callStatus = row.getAs[String]("call_status")
        val duration = row.getAs[Int]("duration")
        val callStartAt = row.getAs[Long]("timestamp")
        val callEndAt = row.getAs[Long]("end_call_at")

        val clickAndCallDurationGap = callStartAt - clickedAt

        new GenericRowWithSchema(
          Array(
            clickedAt,
            messageId,
            anonymousId,
            userId,
            userPhoneNumber,
            url,
            sourceType,
            sourceCode,
            event,
            os,
            platform,
            callId,
            callerPhone,
            receiverPhone,
            extension,
            callStatus,
            duration,
            callStartAt,
            callEndAt,
            clickAndCallDurationGap
          ),
          schema
        )
      case None =>
        new GenericRowWithSchema(
          Array(
            clickedAt,
            messageId,
            anonymousId,
            userId,
            userPhoneNumber,
            url,
            sourceType,
            sourceCode,
            event,
            os,
            platform,
            "",
            "",
            "",
            "",
            "",
            0,
            0L,
            0L,
            0L
          ),
          schema
        )
    }

  }

  private def enhanceRealPhoneFromExt(df: DataFrame, callExtensionDf: DataFrame): DataFrame = {

    df
      .joinWith(callExtensionDf, df("receiver_phone") === callExtensionDf("extension"), "left")
      .select(
        col("_1.clicked_at"),
        col("_1.message_id"),
        col("_1.anonymous_id"),
        col("_1.user_id"),
        col("_1.user_phone_number"),
        col("_1.url"),
        col("_1.source_type"),
        col("_1.source_code"),
        col("_1.event"),
        col("_1.os"),
        col("_1.platform"),
        col("_1.call_id"),
        col("_1.caller_phone"),
        when(col("_2.phone_number").isNotNull, col("_2.phone_number"))
          .otherwise(col("_1.receiver_phone"))
        .as("receiver_phone"),
        col("_1.extension"),
        col("_1.call_status"),
        col("_1.duration"),
        col("_1.call_start_at"),
        col("_1.call_end_at"),
        col("_1.click_and_call_duration_gap")
      )
  }
}
