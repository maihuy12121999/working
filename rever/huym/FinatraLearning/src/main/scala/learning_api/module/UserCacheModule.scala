package learning_api.module

import javax.inject.Singleton

import com.google.inject.Provides
import com.twitter.inject.TwitterModule
import learning_api.domain.{UserID, UserInfo}
import learning_api.repository.{CacheRepository, OnMemoryCacheRepository}
import learning_api.service.{UserCacheService, UserCacheServiceImpl}
/**
  * Created by SangDang on 9/16/16.
  */
object   UserCacheModule extends TwitterModule {
  override def configure: Unit = {
    bind[UserCacheService].to[UserCacheServiceImpl]
//    bind[CacheRepository[UserID, UserInfo]].to[OnMemoryCacheRepository[UserID, UserInfo]]
  }
  @Singleton
  @Provides
  def providesUserCacheRepository(): CacheRepository[UserID, UserInfo] = {
    new OnMemoryCacheRepository[UserID, UserInfo]()
  }
}
