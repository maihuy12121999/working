package rever.rsparkflow.spark.domain.sale_pipeline

import com.fasterxml.jackson.databind.JsonNode

case class SalePipeline(node: JsonNode) {
  final lazy val id = node.at("/id").asInt()
  final lazy val serviceType = PipelineServiceType(
    node.at("/service_type/value").asInt(),
    node.at("/service_type/name").asText(""),
    node.at("/service_type/original_name").asText("")
  )

  final lazy val name = node.at("/name").asText("")
  final lazy val description = Option(node.at("/description").asText())
  final lazy val createdBy = Option(node.at("/created_by").asText())
  final lazy val updatedBy = Option(node.at("/updated_by").asText())
  final lazy val createdTime = Option(node.at("/created_time").asLong())
  final lazy val updatedTime = Option(node.at("/updated_time").asLong())
  final lazy val status = node.at("/status").asInt()

}

case class PipelineServiceType(id: Int, name: String, originName: String) {

  /**
   *
   * @return [sale, rent, ]
   */
  def detectServiceType(): String = {
    val pipelineServiceType = name.toLowerCase()
    if (pipelineServiceType.contains("buying")) {
      "sale"
    } else if (pipelineServiceType.contains("rent")) {
      "rent"
    } else {
      ""
    }
  }

  /**
   *
   * @return [primary, secondary,]
   */
  def detectBusinessUnit(): String = {
    val pipelineServiceType = name.toLowerCase()
    if (pipelineServiceType.contains("primary")) {
      "primary"
    } else if (pipelineServiceType.contains("secondary") || pipelineServiceType.equalsIgnoreCase("rent")) {
      "secondary"
    } else {
      ""
    }
  }
}
