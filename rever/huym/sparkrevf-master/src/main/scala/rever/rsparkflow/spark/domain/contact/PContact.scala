package rever.rsparkflow.spark.domain.contact

import com.fasterxml.jackson.databind.JsonNode

case class PContact(
    pCid: String,
    cid: Option[String],
    status: Option[Int],
    owner: String,
    createdTime: Option[Long],
    updatedTime: Option[Long],
    properties: JsonNode
)
