package rever.rsparkflow.spark.api.udf

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.api.java.{UDF1, UDF2, UDF3}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, RowFactory}
import rever.rsparkflow.spark.utils.{JsonUtils, TimestampUtils}
import ua_parser.{Client, Parser}

import java.net.URL

/**
  * @author anhlt (andy)
  * @since 28/05/2022
  * */

case class CoalesceUdf() extends UDF2[String, String, String] {

  override def call(columnValue: String, defaultValue: String): String =
    try {
      if (columnValue == null || columnValue.isEmpty) return defaultValue
      columnValue
    } catch {
      case ex: Exception =>
        defaultValue
    }
}

case class DomainUdf() extends UDF2[String, String, String] {

  override def call(url: String, defaultValue: String): String =
    try {
      val u = new URL(url)
      u.getHost
    } catch {
      case ex: Exception =>
        defaultValue
    }
}

object UserAgentUdf {
  final val outputDataType: StructType = StructType(
    Array(
      StructField("browser", DataTypes.StringType, nullable = true),
      StructField("browser_version", DataTypes.StringType, nullable = true),
      StructField("os", DataTypes.StringType, nullable = true),
      StructField("os_version", DataTypes.StringType, nullable = true),
      StructField("device", DataTypes.StringType, nullable = true),
      StructField("platform", DataTypes.StringType, nullable = true)
    )
  )

  def detectOsPlatform(device: String, os: String, osVersion: String): Option[String] = {
    def isSmartTvDevice(device: String): Boolean = {
      Seq(
        device.toLowerCase.contains("smart-tv"),
        device.toLowerCase.contains("smarttv"),
        device.toLowerCase.contains("android tv")
      ).exists(flag => flag)
    }
    try {
      val deviceFamily = device.toLowerCase
      val osFamily = os.toLowerCase

      val platform = osFamily match {
        case os if isSmartTvDevice(deviceFamily)                          => "SmartTV"
        case "windows" if "CE".equals(osVersion)                          => "Mobile"
        case "ios" | "android" | "windows phone"                          => "Mobile"
        case "nokia series 40" | "nokia series 60"                        => "Mobile"
        case "blackberry os"                                              => "Mobile"
        case _ if deviceFamily.contains("smartphone")                     => "Mobile"
        case os if os.contains("mac")                                     => "Desktop"
        case "windows" | "ubuntu" | "linux" | "solaris"                   => "Desktop"
        case "debian" | "fedora" | "netbsd"                               => "Desktop"
        case "chrome os"                                                  => "Desktop"
        case os if os.contains("nokia") && deviceFamily.contains("nokia") => "Mobile"
        case os if deviceFamily.contains("nokia")                         => "Mobile"
        case "other" =>
          if (deviceFamily.contains("samsung sm") || deviceFamily.contains("feature phone"))
            "Mobile"
          else
            "Other"
        case _ => "Other"
      }
      Some(platform)
    } catch {
      case _ => None
    }
  }
}

case class UserAgentUdf() extends UDF1[String, Row] {

  override def call(userAgent: String): Row = {
    val parser: Parser = new Parser
    val clientInfo: Client = parser.parse(userAgent)
    RowFactory.create(
      Option(clientInfo.userAgent).map(_.family).filterNot(_ == null).map(_.trim).getOrElse(""),
      Option(clientInfo.userAgent).map(_.major).filterNot(_ == null).map(_.trim).getOrElse(""),
      Option(clientInfo.os).map(_.family).filterNot(_ == null).map(_.trim).getOrElse(""),
      Option(clientInfo.os).map(_.major).filterNot(_ == null).map(_.trim).getOrElse(""),
      Option(clientInfo.device).map(_.family).filterNot(_ == null).map(_.trim).getOrElse(""),
      UserAgentUdf
        .detectOsPlatform(clientInfo.device.family, clientInfo.os.family, clientInfo.os.major)
        .map(_.trim)
        .getOrElse("")
    )
  }

}

case class DateToMillisUdf() extends UDF2[String, String, java.lang.Long] {

  override def call(dateStr: String, format: String): java.lang.Long = {
    try {
      TimestampUtils.parseMillsFromString(dateStr, format)
    } catch {
      case ex: Exception => null
    }
  }
}

object SalePipelineCustomerUdf {
  final val outputDataType: StructType = StructType(
    Array(
      StructField("type", DataTypes.StringType, nullable = false),
      StructField("number", DataTypes.StringType, nullable = false),
      StructField("customer_id", DataTypes.StringType, nullable = false),
      StructField("name", DataTypes.StringType, nullable = false),
      StructField("transaction_type", DataTypes.StringType, nullable = false)
    )
  )
}

case class SalePipelineCustomerUdf() extends UDF1[String, Row] {

  override def call(customerJsonStr: String): Row = {

    val node = JsonUtils.fromJson[JsonNode](customerJsonStr)

    val name = node.at("/name").asText("")
    val idType = node.at("/type").asText("")
    val idNumber = node.at("/number").asText("").trim
    val customerId = node.at("/customer_id").asText("").trim
    val transactionType = node.at("/transaction_type").asText("").trim

    RowFactory.create(
      idType,
      idNumber,
      customerId,
      name,
      transactionType
    )
  }
}

case class DetectBusinessUnitUdf() extends UDF3[String, String, String, String] {

  override def call(businessUnit: String, marketCenterName: String, teamName: String): String = {
    try {
      businessUnit match {
        case "primary" | "secondary" => businessUnit
        case _ =>
          val name = Seq(marketCenterName, teamName)
            .filterNot(_ == null)
            .filterNot(_.trim.isEmpty)
            .headOption
            .getOrElse("")
            .toLowerCase
          name match {
            case "pdn"                                           => "primary"
            case name if name.matches("p(\\d+)(\\s*-\\s*\\d+)?") => "primary"
            case name if name.matches("a(\\d+)(\\s*-\\s*\\d+)?") => "secondary"
            case _                                               => "unknown"
          }
      }
    } catch {
      case ex: Exception => null
    }
  }
}
