package rever.etl.data_sync.util

import com.fasterxml.jackson.databind.JsonNode
import rever.etl.data_sync.domain.EmploymentStatus
import rever.etl.rsparkflow.utils.TimestampUtils

object EmploymentStatusUtils {

  def fromJsonNode(node: JsonNode): EmploymentStatus = {
    val effectiveDate =
      Option(node.at("/effective_date").asText(null)).getOrElse(throw new Exception("No effective_date "))
    val effectiveTime = TimestampUtils.parseMillsFromString(effectiveDate, "dd/MM/yyyy")
    val status = Option(node.at("/status").asText(null)).getOrElse(throw new Exception("No status "))
    val comment = node.at("/comment").asText("")

    EmploymentStatus(
      status,
      effectiveDate,
      effectiveTime,
      comment = Some(comment)
    )
  }
}
