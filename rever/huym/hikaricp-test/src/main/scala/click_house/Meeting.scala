package scala.click_house

case class Meeting(var engagementType:String ="", var marketCenterId:String="",var teamId:String="",var agentId:String="",var source:String=""){
  def show(): Unit={
    println(s"Meeting(type=$engagementType,market_center_id=$marketCenterId,team_id=$teamId,agent_id=$agentId,source=$source)")
  }
}
