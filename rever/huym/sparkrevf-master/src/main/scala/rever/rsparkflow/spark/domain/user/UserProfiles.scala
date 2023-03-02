package rever.rsparkflow.spark.domain.user

import com.fasterxml.jackson.databind.JsonNode

/**
  * @author anhlt (andy)
  * @since 26/05/2022
**/
case class User(userNode: JsonNode) {

  lazy final val username = userNode.at("/username").asText("")
  lazy final val employeeId = userNode.at("/properties/employee_id").asText("")
  lazy final val jobTitle = userNode.at("/properties/job_title").asText("")
  lazy final val phone = userNode.at("/properties/phone").asText("")
  lazy final val marketCenterId = userNode.at("/properties/market_center").asText("")
  lazy final val status = userNode.at("/status").asInt(0)

  def workEmail(defaultValue: String = "unknown"): String = {
    userNode
      .at("/properties/work_email")
      .asText(defaultValue)
  }

  def teamId(defaultValue: String = "unknown"): String = {
    userNode
      .at("/properties/team")
      .asText(defaultValue)
  }
}

case class Team(teamNode: JsonNode) {
  def teamId(defaultValue: String = "unknown"): String = {
    teamNode
      .at("/id")
      .asText(defaultValue)
  }
  lazy final val name = teamNode.at("/name").asText("")
  lazy final val `type` = teamNode.at("/type").asInt(20)
  lazy final val officeId = teamNode.at("/properties/office").asText("")
  lazy final val status = teamNode.at("/status").asInt(0)

}
