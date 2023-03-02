package learning_api
import com.twitter.finagle.http.Request
import com.twitter.finagle.mux.Response
import com.twitter.finatra.http.HttpServer
import com.twitter.finatra.http.filters.CommonFilters
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.thrift.ThriftServer
import com.twitter.finatra.thrift.routing.ThriftRouter
import learning_api.controller.http
import learning_api.controller.http.{HealthController, TestController}
import learning_api.controller.thrift.CacheController
import learning_api.module.UserCacheModule
import learning_api.util.ZConfig
/**
  * Created by SangDang on 9/8/
  **/
object MainApp extends Server
class Server extends HttpServer with ThriftServer {

 override protected def defaultHttpPort: String = ZConfig.getString("server.http.port",":8080")

  override protected def defaultThriftPort: String = ZConfig.getString("server.thrift.port",":8081")

  override protected def disableAdminHttpServer: Boolean = ZConfig.getBoolean("server.admin.disable",true)
  override val modules = Seq(UserCacheModule)

  override protected def configureHttp(router: HttpRouter): Unit = {
    router
      .filter[CommonFilters]
      .add[http.CacheController]
      .add[HealthController]
      .add[TestController]
  }
  override protected def configureThrift(router: ThriftRouter): Unit = {
    router
      .add[CacheController]
  }
}
