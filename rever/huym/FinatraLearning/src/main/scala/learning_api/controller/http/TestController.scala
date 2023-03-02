package learning_api.controller.http
import com.twitter.finagle.Mux.{Server, server}
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.Controller
import com.twitter.finatra.http.request.RequestUtils
import com.twitter.finatra.jackson.ScalaObjectMapper
import com.twitter.finatra.jackson.modules.ScalaObjectMapperModule
import learning_api.Server
import learning_api.domain.{AcceptsHeaderRequest, FormPostRequest, FromRouteRequest, GetCacheRequest, IdAndNameRequest, IdRequest, PutCacheRequest, QueryParamRequest, UserID, UserInfo, UserInfoDataBase, hiRequest}
import learning_api.service.UserCacheService

import java.io.{IOException, PrintWriter}
import java.net.CacheRequest
import javax.inject.Inject
class TestController @Inject()(exampleService : UserCacheService) extends Controller{
  val userInfoDatabase:UserInfoDataBase = new UserInfoDataBase
  userInfoDatabase.add(UserInfo(UserID("2"),"Yen Nhi",22,"female"))
  val mapper: ScalaObjectMapper = (new ScalaObjectMapperModule).objectMapper
//  =============================================RouteParam=========================================================
  get("/routeParam/:nameTeam/:id"){
    request: FromRouteRequest=>
      s"The best team in the world is ${request.name} having the ID ${request.id} "
  }
  get("/groups/:id"){
    request : Request=>
      logger.info("this is my id: " + request.params("id"))
      "this is my id: " + request.getParam("id")
  }
  get("/heat/"){
    request : Request=>
//      response.ok.header("a","b")
//        .json("""
//          {
//            "name": "Bob",
//            "age": 19
//          }
//          """)
//      response.ok.body("bob")
//      response.status(999).body("ssss")
//      response.temporaryRedirect.location("/groups/1")
  }
  post("/3"){
    request:hiRequest=>
      s"The ID is ${request.id}, Name is ${request.name}"
  }
  post("/formPost/:id"){
    request:FormPostRequest=>
      s"The ID is ${request.cardId}, Name is ${request.name}, Age is ${request.age}"

  }
  post("/redirect"){
    request:Request=>
      response
  }
  put("/echo") { request: Request =>
    response.ok(request.contentString)
  }
  put("/put_route_param/:id") {
    request: IdRequest =>
      request.id + "_" + request.request.contentString
  }
  put("/put_route_param_and_name/:id") {
    request: IdAndNameRequest =>
      request.id + "_" + request.name
  }
  get("/chovy"){
    request : QueryParamRequest =>
//      s"""
//      |Chovy stats
//      | - Chovy's score = ${request.score}
//      | - Chovy champion ? = ${request.isChampion}
//      | - Chovy dominance = ${request.stats}""".stripMargin
      Map(
        "Chovy-score"->request.score,
        "Chovy-champion"->request.isChampion,
        "Chovy-dominance"->request.stats
      )
  }
  get("/acceptHeaders") { request: AcceptsHeaderRequest =>
    Map(
      "Accept" -> request.accept,
      "Accept-Charset" -> request.acceptCharset,
      "Accept-Charset-Again" -> request.acceptCharsetAgain,
      "Accept-Encoding" -> request.acceptEncoding
    )
  }
  post("/multipartParamsEcho") { request: Request =>
    RequestUtils.multiParams(request).keys
  }
  patch("/patch") { request: Request =>
    request.content
  }
  options("/options/:id") { request: Request =>
    logger.info(request.params("id"))
    request.params("id")
  }
  get("/") {
    request:Request =>
      val loggedIn = request.cookies.getValue("loggedIn").getOrElse("false")
      response.ok.
        plain("logged in?:" + loggedIn)
  }
  get("/NotFound") { request: Request =>
    response.notFound("abc not found").toFutureException
  }
  get("/ServerError") { request: Request =>
    response.internalServerError.toFutureException
  }
  post("/addUser/"){
    userInfo:UserInfo=>
      val id = userInfo.userID.id
      val ids = userInfoDatabase.userInfoList.map(_.userID.id)
      if (!ids.contains(id)){
        userInfoDatabase.add(userInfo)
        userInfoDatabase
      }
      else{
        "The id has already existed"
      }
  }
  get("/userInfos/"){
    _:Request=>
      mapper.writePrettyString(userInfoDatabase)
  }
  get("/removeUser/:id"){
    request:Request=>
      val id = request.getParam("id")
      try {
        userInfoDatabase.remove(id)
        userInfoDatabase
      }
      catch {
        case e: IOException => "Remove failed"
        case _ => "The Id is not valid"
      }
  }
}