package learning_api.controller

import com.twitter.finagle.http.{MediaType, Status}
import com.twitter.finagle.http.Status.Ok
import com.twitter.finatra.http.EmbeddedHttpServer
import com.twitter.inject.server.FeatureTest
import learning_api.Server

class TestControllerTest extends FeatureTest {
  override protected val server = new EmbeddedHttpServer(twitterServer = new Server) with com.twitter.finatra.thrift.ThriftClient
  test("RouteParam_CustomRequestClass") {
    server.httpGet(path = "/routeParam/Barca/2", andExpect = Status.Ok)
  }
  test("RouteParam_FinagleRequest") {
    server.httpGet(path = "/groups/1", andExpect = Status.Ok)
  }
  test("QueryParam") {
    server.httpGet(path = "/chovy?score=100&champion=true&stats=100,100,100", andExpect = Status.Ok)
  }
  test("PostMethod") {
    server.httpPost(
      path = "/3",
      andExpect = Status.Ok,
      postBody =
        """
            {
              "id":"1",
              "name":"mai huy"
            }
        """.stripMargin
    )
  }
  test("Header") {
    server.httpGet(path = "/acceptHeaders", andExpect = Status.Ok,
      headers = Map("accept-charset"->"123","accept"->"ac","Accept-Encoding"->"12E"))
  }
  test("FormPost") {
    server.httpFormPost(
      path = "/formPost/1",
      andExpect = Status.Ok,
      params = Map("name"->"mai huy","age"->"18")
    )
  }
  test("Put with route_param and content string") {
    server.httpPut(
      path = "/put_route_param/1",
      andExpect = Status.Ok,
      putBody = "huy",
      contentType = "plain/text"
    )
  }
  test("Put with route_param_and_name") {
    server.httpPut(
      path = "/put_route_param_and_name/1",
      andExpect = Status.Ok,
      putBody = """{"name":"huy"}""",
    )
  }
  test("PATCH") {
    server.httpPatch(
      "/patch",
      contentType = MediaType.PlainTextUtf8,
      patchBody = "asdf",
      andExpect = Ok,
    )
  }
  test("Patch with json body") {
    server.httpPatch(
      "/patch",
      patchBody = "{\"id\" : \"11\"}",
      andExpect = Ok
    )
  }
  test("OPTIONS") {
    server.httpOptions("/options/1", andExpect = Ok)
  }
}
