package rever.rsparkflow.spark.domain.contact

import com.fasterxml.jackson.databind.JsonNode

case class CoreContact(
    cid: String,
    status: Int,
    createdTime: Long,
    updatedTime: Long,
    properties: JsonNode
)
