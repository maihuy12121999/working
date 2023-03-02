package rever.etl.support.rever_academy

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, functions}
import rever.rsparkflow.spark.FlowMixin
import rever.rsparkflow.spark.api.annotation.{Output, Table}
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.client.DataMappingClient
import rever.rsparkflow.spark.utils.{JsonUtils, TimestampUtils}
import rever.etl.support.rever_academy.reader.ReverUserReader
import rever.etl.support.rever_academy.writer.ReverUserWriter

import scala.collection.JavaConverters.asScalaIteratorConverter

class ExportStaff extends FlowMixin {

  @Output(writer = classOf[ReverUserWriter])
  @Table("academy_staff")
  def build(
      @Table(
        name = "rever_user_profiles",
        reader = classOf[ReverUserReader]
      ) df: DataFrame,
      config: Config
  ): DataFrame = {

    val ignoreStaffQuitBefore = config.get("ignore_staff_quit_before_date", "2021-01-01")

    val dateFormatUdf = functions.udf[String, String]((date: String) => {
      try {
        val t = TimestampUtils.parseMillsFromString(date, "dd/MM/yyyy")
        TimestampUtils.format(t, Some("yyyy-MM-dd"))
      } catch {
        case _ => ""
      }
    })

    val hireDateUdf = functions.udf[String, String, String](enhanceHireDate)
    val quitDateUdf = functions.udf[String, Long, String](enhanceQuitDate)
    val quiteDateToMilliUdf = udf[Long, String]((quitDate: String) => {
      try {
        TimestampUtils.parseMillsFromString(quitDate, "yyyy-MM-dd")
      } catch {
        case _ => 0L
      }
    })

    val enhanceDf = df
      .where(col("work_email").isNotNull)
      .where(col("work_email").notEqual(""))
      .withColumn("birthday", dateFormatUdf(col("birthday")))
      .withColumn("hire_date", hireDateUdf(col("hire_date"), col("employment_status")))
      .withColumn("quit_date", quitDateUdf(col("status"), col("employment_status")))
      .drop("employment_status")
      .where(
        not(
          col("status")
            .equalTo(0)
            .and(
              quiteDateToMilliUdf(col("quit_date")).leq(
                TimestampUtils.parseMillsFromString(ignoreStaffQuitBefore, "yyyy-MM-dd")
              )
            )
        )
      )
      .sort(col("status").desc, col("work_email"))
      .withColumn(
        "status",
        when(col("status").equalTo("1"), lit("Đang hoạt động"))
          .otherwise(lit("Ngưng hoạt động"))
      )

    enhanceDf.mapPartitions(rows => enhanceJobTitle(rows.toSeq, config))(RowEncoder(enhanceDf.schema))
  }

  private def enhanceHireDate(hireDate: String, employmentStatus: String): String = {

    val employmentStatusNode = parseEmploymentStatus(employmentStatus)

    val originHireDateOpt =
      try {
        val t = TimestampUtils.parseMillsFromString(hireDate, "dd/MM/yyyy")
        Some(TimestampUtils.format(t, Some("yyyy-MM-dd")))
      } catch {
        case _ => None
      }

    val actualHireDateOpt = employmentStatusNode.reverse
      .find(e => e._1.equals("probation") || e._1.equals("official"))
      .map(_._2)

    Seq(
      actualHireDateOpt,
      originHireDateOpt
    ).filter(_.isDefined).flatten.headOption.getOrElse(hireDate)

  }

  private def enhanceQuitDate(status: Long, employmentStatus: String): String = {

    if (status != 1) {
      val employmentStatusNode = parseEmploymentStatus(employmentStatus)
      employmentStatusNode.reverse
        .find({
          case ("quit", _) => true
          case _           => false
        })
        .map(_._2)
        .getOrElse("")
    } else {
      ""
    }

  }

  private def enhanceJobTitle(rows: Seq[Row], config: Config): Iterator[Row] = {

    val client = DataMappingClient.client(config)

    val jobTitleIds = rows
      .map(row => row.getAs[String]("job_title_id"))
      .filterNot(_ == null)
      .filterNot(_.isEmpty)

    val dataMap = client.mGetJobTitleByAlias(jobTitleIds)

    rows
      .map(row => {
        val workEmail = row.getAs[String]("work_email")
        val fullName = row.getAs[String]("full_name")
        val employeeId = row.getAs[String]("employee_id")
        val birthday = row.getAs[String]("birthday")
        val jobTitleId = row.getAs[String]("job_title_id")
        val jobTitle = dataMap.getOrElse(jobTitleId, "")

        val hireDate = row.getAs[String]("hire_date")
        val lineManager = row.getAs[String]("line_manager")
        val lineManagerEmail = row.getAs[String]("line_manager_email")
        val team = row.getAs[String]("team")
        val department = row.getAs[String]("department")
        val office = row.getAs[String]("office")
        val status = row.getAs[String]("status")
        val quitDate = row.getAs[String]("quit_date")

        Row(
          workEmail,
          fullName,
          employeeId,
          birthday,
          jobTitleId,
          jobTitle,
          hireDate,
          lineManager,
          lineManagerEmail,
          team,
          department,
          office,
          status,
          quitDate
        )
      })
      .toIterator

  }

  private def parseEmploymentStatus(s: String): Seq[(String, String)] = {
    try {
      JsonUtils
        .fromJson[JsonNode](s)
        .elements()
        .asScala
        .map(node => {
          val status = node.at("/status").asText()
          val effectiveDate = Option(node.at("/effective_date").asText())
            .map(TimestampUtils.parseMillsFromString(_, "dd/MM/yyyy"))
          status -> effectiveDate
        })
        .toSeq
        .sortBy(_._2.getOrElse(0L))(Ordering.Long)
        .filterNot(_._1 == null)
        .filterNot(_._1.isEmpty)
        .filter(_._2.isDefined)
        .map(e => e._1 -> TimestampUtils.format(e._2.getOrElse(0L), Some("yyyy-MM-dd")))
    } catch {
      case _ => Seq.empty
    }
  }

}
