package learning_api.controller

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.twitter.finagle.http.Status
import com.twitter.finatra.http.EmbeddedHttpServer
import com.twitter.finatra.jackson.ScalaObjectMapper
import com.twitter.finatra.jackson.modules.ScalaObjectMapperModule
import com.twitter.inject.server.FeatureTest
import learning_api.Server
import learning_api.domain.{UserID, UserInfo, UserInfoDataBase}

import scala.tools.nsc.doc.model.diagram.ObjectNode
class JsonTest extends FeatureTest{
  override protected val server = new EmbeddedHttpServer(twitterServer = new Server) with com.twitter.finatra.thrift.ThriftClient
  val userInfoDatabase:UserInfoDataBase = new UserInfoDataBase
  userInfoDatabase.add(UserInfo(UserID("2"),"Yen Nhi",22,"female"))
  val mapper: ScalaObjectMapper = (new ScalaObjectMapperModule).objectMapper
  test("Scala class -> string"){
    print(mapper.writePrettyString(userInfoDatabase))
  }
  test("String -> Scala class"){
    val stringJson =
      """
        |{
        |  "user_info_list" : [
        |    {
        |      "user_id" : {
        |        "id" : "1"
        |      },
        |      "user_name" : "mai huy",
        |      "age" : 23,
        |      "sex" : "male"
        |    }
        |  ]
        |}
        |""".stripMargin
    val userInfoList :UserInfoDataBase = mapper.parse[UserInfoDataBase](stringJson)
    println(userInfoList)
  }
  test("String -> Json") {
    val stringJson =
      """
        |{
        |  "user_info_list" : [
        |    {
        |      "user_id" : {
        |        "id" : "1"
        |      },
        |      "user_name" : "mai huy",
        |      "age" : 23,
        |      "sex" : "male"
        |    },
        |    {
        |      "user_id" : {
        |        "id" : "2"
        |      },
        |      "user_name" : "Yen Nhi",
        |      "age" : 22,
        |      "sex" : "female"
        |    }
        |  ]
        |}
        |""".stripMargin
    val userInfoJsonS: JsonNode = mapper.parse[JsonNode](stringJson)
    println(userInfoJsonS.toPrettyString)
    println(userInfoJsonS.get("user_info_list"))
    println(userInfoJsonS.get("user_info_list").get(0).get("age").asInt())
    println(userInfoJsonS.get("user_info_list").get(0).get("user_id").get("id").asLong())
  }
  test("Json Integration get userData"){
    server.httpGet(
      path = "/userInfos/",
      andExpect = Status.Ok
    )
  }
  test("Json Integration add user"){
    server.httpPost(
      path = "/addUser/",
      andExpect = Status.Ok,
      postBody =
        """
          | {
          |   "user_id" : {
          |     "id" : "3"
          |    },
          |   "user_name" : "Yen Nhi",
          |   "age" : 22,
          |   "sex" : "female"
          | }
          |""".stripMargin
    )
  }
  test("Json Integration remove user"){
    server.httpGet(
      path = "/removeUser/2",
      andExpect = Status.Ok
    )
  }
  test("Crete Edit update Json "){
    val stringJson =
      """
    |    {
    |      "user_id" : {
    |        "id" : "1"
    |      },
    |      "user_name" : "mai huy",
    |      "age" : 23,
    |      "sex" : "male"
    |    }
    |""".stripMargin
    val userInfoJsonS : JsonNode = mapper.parse[JsonNode](stringJson)
    val userInfoObject: ObjectNode = mapper.parse[ObjectNode](stringJson)
    userInfoObject.put("user_name","Yen Nhi")
    userInfoObject.put("nationality","Viet Nam")
    println(mapper.parse[ObjectNode](userInfoObject.get("user_id")).put("id",100))
    println(userInfoObject.toPrettyString)
  }
}
