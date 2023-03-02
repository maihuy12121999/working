package learning_api.domain

import com.twitter.finatra.http.annotations.{FormParam, Header, QueryParam, RouteParam}
import com.twitter.finatra.validation.constraints.Max
import org.joda.time.DateTime

case class FromRouteRequest (
  @RouteParam("nameTeam") name:String,
  @RouteParam id:String
)
case class QueryParamRequest (
  @Max(100) @QueryParam score:Int,
//  @QueryParam startDate: Option[DateTime],
  @QueryParam("champion") isChampion : Boolean,
  @QueryParam(commaSeparatedList = true) stats : Seq[Long]
)
case class AcceptsHeaderRequest(
   @Header accept: String,
   @Header("accept-charset") acceptCharset: String,
   @Header("Accept-Charset") acceptCharsetAgain: String,
   @Header("Accept-Encoding") acceptEncoding: String)

case class FormPostRequest(@FormParam name: String, @FormParam age: Long, @RouteParam("id") cardId: String)
