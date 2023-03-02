package scala.click_house

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import scala.collection.mutable.ListBuffer

object Main {
  final val clickHouseConfig = new HikariConfig(s"config/ch_config.properties")
  final val dataSource: HikariDataSource =  new HikariDataSource(clickHouseConfig)
  def main(args: Array[String]): Unit = {
    val meetings = fetchData()
    meetings.foreach(_.show())
  }
  def fetchData():ListBuffer[Meeting] ={
    val sqlQuery = initQuery()
    val meetings = ListBuffer.empty[Meeting]
    var con:Connection =null
    var pst:PreparedStatement = null
    var resultSet:ResultSet = null
    try {
      con= dataSource.getConnection
      pst= con.prepareStatement(sqlQuery)
      resultSet= pst.executeQuery()
      while (resultSet.next()) {
        val engagementType = resultSet.getString("type")
        val marketCenterId = resultSet.getString("market_center_id")
        val teamId = resultSet.getString("team_id")
        val agentId = resultSet.getString("agent_id")
        val source = resultSet.getString("source")
        val meeting = new Meeting(engagementType,marketCenterId,teamId,agentId,source)
        meetings.append(meeting)
      }
    }
    catch{
      case e:SQLException=>e.printStackTrace()
    }
    finally{
      if(con !=null) con.close()
      if(pst!=null) pst.close()
      if(resultSet!=null) resultSet.close()
    }
    meetings
  }
  def initQuery():String ={
    s"""
       |SELECT
       |        IF (JSONHas(data,'type'),JSONExtractString(data, 'type'),JSONExtractString(old_data, 'type')) as type,
       |        IF (JSONHas(data,'owner_market_center'),JSONExtractString(data, 'owner_market_center'),JSONExtractString(old_data, 'owner_market_center')) as market_center_id,
       |        IF (JSONHas(data,'owner_team'),JSONExtractString(data,'owner_team'),JSONExtractString(old_data,'owner_team')) as team_id,
       |        IF (JSONHas(data,'owner'),JSONExtractString(data,'owner'),JSONExtractString(old_data,'owner')) as agent_id,
       |        IF (JSONHas(data,'source'),JSONExtractString(data,'source'),JSONExtractString(old_data,'source')) as source
       |FROM engagement_historical_1
       |WHERE 1=1
       |    AND type = 'meeting'
       |LIMIT 0,10
       |""".stripMargin
  }

}
