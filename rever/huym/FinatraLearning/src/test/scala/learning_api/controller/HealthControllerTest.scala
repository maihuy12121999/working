package learning_api.controller

import com.twitter.finagle.http.Status
import com.twitter.finatra.http.EmbeddedHttpServer
import com.twitter.finatra.thrift.ThriftClient
import com.twitter.inject.server.FeatureTest
import learning_api.Server

/**
  * Created by SangDang on 9/18/16.
  */
class HealthControllerTest extends FeatureTest{
  override protected val server = new EmbeddedHttpServer(twitterServer = new Server) with ThriftClient

  test("Test Health Http") {
    server.httpGet(path = "/ping",andExpect = Status.Ok, withJsonBody =
      """
        {
          "status":"ok",
          "data":"pong"
        }
      """.stripMargin)
  }
}