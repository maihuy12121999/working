package learning_api.controller.http

import com.twitter.finagle.http.Request
import com.twitter.finatra.http.Controller

/**
  * Created by SangDang on 9/18/16.
  */
class HealthController extends Controller {
  get("/ping") {
    _: Request => {
      logger.info("ping")
      response.ok(Map("status" -> "ok", "data" -> "pong"))
    }
  }
}
