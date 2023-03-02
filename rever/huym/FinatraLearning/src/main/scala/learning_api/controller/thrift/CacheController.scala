package learning_api.controller.thrift

import com.twitter.finatra.thrift.Controller
import com.twitter.inject.Logging
import com.twitter.scrooge.{Request, Response}
import com.twitter.util.Future
import javax.inject.{Inject, Singleton}
import learning_api.service.{TUserCacheService, UserCacheService}
import learning_api.service.TUserCacheService._
import learning_api.domain.ThriftImplicit._
/**
  * Created by SangDang on 9/16/16.
  */
@Singleton
class CacheController @Inject()(cacheService: UserCacheService) extends Controller(TUserCacheService) with Logging {

  handle(AddUser).withFn {
    req: Request[AddUser.Args] => {
      val args = req.args
      Future.value(cacheService.addUser(args.userInfo.userId, args.userInfo))
        .map(v => Response(v))
    }
  }
  handle(GetUser).withFn {
    req: Request[GetUser.Args] => {
      val args = req.args
      cacheService.getUser(args.userId)
        .map(v => Response(v))
    }
  }
}