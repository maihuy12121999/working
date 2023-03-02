import org.scalatest.FunSuite
import rever.etl.data_sync.domain.rever_search.Point
import rever.etl.data_sync.util.CentralPointUtils

class CentralPointTest extends FunSuite{
  test("Test method getting central point"){
    assertResult(Point(5.0,7.0))(CentralPointUtils.getCenterPoint(List(Point(3,4),Point(1,10),Point(9,9))))
    assertResult(Point(6.0,13.0/2))(CentralPointUtils.getCenterPoint(List(Point(5,4),Point(3,7),Point(9,9))))

  }
}
