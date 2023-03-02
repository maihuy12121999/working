package learning_api.domain

import com.twitter.finagle.http.Request
import com.twitter.finatra.http.annotations.RouteParam

case class IdRequest (@RouteParam id:Long, request: Request)
case class IdAndNameRequest (@RouteParam id:Long, name:String)
