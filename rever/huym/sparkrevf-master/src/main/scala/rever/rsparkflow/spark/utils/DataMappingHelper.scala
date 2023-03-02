package rever.rsparkflow.spark.utils

import rever.rsparkflow.spark.domain.user.{Team, User}

/** @author anhlt (andy)
  * @since 25/05/2022
  */
object DataMappingHelper {

  def getTeamId(teamMap: Map[String, Team], teamName: String): String = {
    teamName match {
      case "f2" | "ctv" | "F2" | "CTV" => teamName.toLowerCase
      case _ =>
        teamMap
          .get(teamName)
          .map(_.teamId())
          .filterNot(_ == null)
          .filterNot(_.isEmpty)
          .getOrElse("unknown")
    }
  }

  def getUserWorkEmail(userMap: Map[String, User], name: String): String = {
    userMap
      .get(name)
      .map(_.workEmail())
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .getOrElse("unknown")
  }

  def getUserTeamId(userMap: Map[String, User], name: String): String = {
    userMap
      .get(name)
      .map(_.teamId())
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .getOrElse("unknown")
  }
}
