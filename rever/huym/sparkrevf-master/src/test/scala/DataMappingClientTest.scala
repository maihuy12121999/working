import org.scalatest.FunSuite
import rever.rsparkflow.spark.client.DataMappingClient

/**
  * @author anhlt
  */
class DataMappingClientTest extends FunSuite {

  private val client = DataMappingClient.client(
//    "http://eks-svc.rever.vn:31462",
    "http://eks-svc.reverland.com:31462",
    "rever",
    "rever@123!"
  )

  test("Parse UserAgent") {
    val userAgent =
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36"
  }

  test("Mget all pipeline") {
    val response = client.mGetSalePipelines()

    response.foreach {
      case (id, pipeline) =>
        println(s"Pipeline: $id - ${pipeline}")
    }
  }

  test("Get frontend user profiles") {
    val response = client.mGetFrontendUserProfiles(Seq("fb-1636689363852000_119"))

    response.foreach {
      case (name, user) =>
        println(s"Name: $name - ${user}")
    }
  }

  test("MGet user by usernames") {
    val response = client.mGetUser(Seq("1638342750117000"))

    response.foreach {
      case (name, user) =>
        println(s"Name: $name - ${user.workEmail()} - ${user.teamId()}")
        println(user.userNode.toPrettyString)
    }
  }

  test("Get user by name") {
    val response = client.mGetUserByName(Seq("Lê Tuấn Anh"))

    response.foreach {
      case (name, user) =>
        println(s"Name: $name - ${user.workEmail()} - ${user.teamId()}")

    }
  }

  test("Mget team") {
    val response = client.mGetTeam(Seq(1164))

    response.foreach {
      case (name, team) =>
        println(s"Team: $name - ${team}")
    }
  }

  test("Get team by name") {
    val response = client.mGetTeamByName(Seq("A02", "P02"))

    response.foreach {
      case (name, team) =>
        println(s"Name: $name - ${team.teamId()}")
    }

  }

  test("Mget Personal Contact Id") {
    val pCids = Seq(
      "1622460967501000",
      "1622516352799000"
    )
    val response = client.mGetPContact(pCids)

    response.foreach {
      case (id, pContact) =>
        println(s"Personal Contact: $id")
        println(pContact)
    }
  }

  test("Mget Sys Contact Id") {
    val cids = Seq(
      "1547143815829509"
    )
    val response = client.mGetSysContact(cids)

    response.foreach {
      case (id, pContact) =>
        println(s"Contact: $id")
        println(pContact)
    }
  }

  test("Mget Inquiry Property Ids") {
    val inquiryIds = Seq(
      "1547143815829509"
    )
    val response = client.mGetInquiryPropertyIds(inquiryIds)

    response.foreach {
      case (id, propertyIds) =>
        println(s"Inquiry Id: $id")
        println(propertyIds)
    }
  }

}
