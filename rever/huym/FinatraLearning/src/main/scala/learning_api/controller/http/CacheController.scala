package learning_api.controller.http

import javax.inject.{Inject, Singleton}

import com.twitter.finatra.http.Controller
import learning_api.domain.{GetCacheRequest, PutCacheRequest, UserID}
import learning_api.service.UserCacheService


/**
  * Created by SangDang on 9/16/16.
  */


class CacheController @Inject()(userCacheService: UserCacheService) extends Controller {
  post("/addUserService") { request: PutCacheRequest =>
    userCacheService.addUser(request.userID, request.userInfo)
    response.ok.body("Add successfully")
  }
  get("/getUserService") {
    request: GetCacheRequest =>
      for {
        userInfo <- userCacheService.getUser(UserID(request.userID))
      } yield {
        response.ok(userInfo)
      }
  }
}