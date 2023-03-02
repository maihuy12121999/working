package rever.etl.data_sync.util

import rever.etl.data_sync.domain.rever_search.Point

object CentralPointUtils{
  def getCenterPoint(points:List[Point]):Point={
    val maxX = points.map(_.lat).max
    val minX = points.map(_.lat).min
    val maxY = points.map(_.lon).max
    val minY = points.map(_.lon).min
    Point((maxX+minX)/2,(maxY+minY)/2)
  }

}
