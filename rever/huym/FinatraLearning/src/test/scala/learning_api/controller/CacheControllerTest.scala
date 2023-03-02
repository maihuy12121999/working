package learning_api.controller

import com.twitter.finagle.http.Status
import com.twitter.finagle.http.Status.Ok
import com.twitter.finatra.http.EmbeddedHttpServer
import com.twitter.inject.server.FeatureTest
import learning_api.Server
import learning_api.domain.thrift.{TUserID, TUserInfo}
import org.scalatest.Assertions
import learning_api.service.TUserCacheService

/**
 * Created by SangDang on 9/18/16.
 */
class CacheControllerTest extends FeatureTest {
  override protected val server = new EmbeddedHttpServer(twitterServer = new Server) with com.twitter.finatra.thrift.ThriftClient
  val thriftClient = server.thriftClient[TUserCacheService.MethodPerEndpoint]("client for test")

  test("[HTTP] Put cache # successful") {
    server.httpPost(
      path = "/addUser",
      postBody =
        """
            {
              "user_id":{
                "id":"1"
              },
              "user_info":{
                "user_id":{
                  "id":"1"
                },
                "user_name":"test_1",
                "age":99,
                "sex":"male"
              }
            }
          """.stripMargin,
      andExpect = Ok
    )
  }

  test("[HTTP] Put cache # be able to get back") {
    server.httpGet(
      path = "/getUser?user_id=1",
      andExpect = Status.Ok,
      withJsonBody =
        """
            {
              "user_id": {
                "id": "1"
              },
              "user_name": "test_1",
              "age": 99,
              "sex": "male"
            }
          """.stripMargin

    )
  }

  test("[Thrift] put cache # successful") {
    com.twitter.util.Await.result(thriftClient.addUser(TUserInfo(TUserID("101"), "test", 100, "male")))
    val userInfo = com.twitter.util.Await.result(thriftClient.getUser(TUserID("101")))
    Assertions.assert(userInfo.userId._1.equals("101"))
    Assertions.assert(userInfo.username.equals("test"))
    Assertions.assert(userInfo.age.equals(100))
    Assertions.assert(userInfo.sex.equals("male"))
  }

  test("[Thrift] external put cache # successful") {

    val client = newClient("localhost", server.thriftExternalPort)

    com.twitter.util.Await.result(client.addUser(TUserInfo(TUserID("111"), "t_test", 101, "female")))
    val userInfo = com.twitter.util.Await.result(client.getUser(TUserID("111")))
    Assertions.assert(userInfo.userId._1.equals("111"))
    Assertions.assert(userInfo.username.equals("t_test"))
    Assertions.assert(userInfo.age.equals(101))
    Assertions.assert(userInfo.sex.equals("female"))
  }

  def newClient(host: String, port: Int, label: String = "") = {
    import com.twitter.finagle.Thrift
    import com.twitter.finagle.service.{Backoff, RetryBudget}
    import com.twitter.finagle.thrift.ClientId
    import com.twitter.util.Duration

    Thrift.client
      .withRequestTimeout(Duration.fromSeconds(5))
      .withSessionQualifier.noFailFast
      .withSessionQualifier.noFailureAccrual
      .withSessionPool.maxSize(10)
      .withSessionPool.minSize(1)
      .withRetryBudget(RetryBudget())
      .withRetryBackoff(Backoff.exponentialJittered(Duration.fromSeconds(5), Duration.fromSeconds(32)))
      .withClientId(ClientId(label))
      .build[TUserCacheService.MethodPerEndpoint](s"$host:$port", label)
  }

}