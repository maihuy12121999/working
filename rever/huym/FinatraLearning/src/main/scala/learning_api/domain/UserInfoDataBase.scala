package learning_api.domain

import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer

case class UserInfoDataBase
(userInfoList: ListBuffer[UserInfo] = ListBuffer(UserInfo(UserID("1"),"mai huy",23,"male"))){
  def add(userInfo:UserInfo): Unit ={
      userInfoList.append(userInfo)
  }
  def remove(id :String): Unit ={
    val index = userInfoList.indexWhere(userInfo=> userInfo.userID.id==id)
    userInfoList.remove(index)
  }
}
