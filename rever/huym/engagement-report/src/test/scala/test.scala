import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import rever.etl.rsparkflow.utils.Utils

import scala.util.matching.Regex

object test {
  def main(args: Array[String]): Unit = {
    val str = "['1578630337914000','1623211019351000','1572956990812000','1619408090223000'" +
      ",'1544811070892253','1544811004148968','1572165540260000','1599982342513000'" +
      ",'1544811256344752','1577524880143000','1544811074640273','1607134911734000','1544897516128323','1570619275923000','1561459001813000']"
//    println(str.dropRight(1).drop(1))
    println(isRegrex(str).toArray.mkString("Array(", ", ", ")"))
    val re = new Regex("[\\[\\'\\]]")
    println(re.replaceAllIn(str,""))
//
    val l = List("Java","Scala","C++")
    println(l.map(("James,,Smith",_,"CA")))
    val list = List(1,2,3)
    println(list.flatMap(x => List((x+1),x+2,x+3)))
    val productTest = Utils.cartesianProduct(List(List("huy","all"),List("Nhi","all"),List("Hanh","all")))
    println(productTest)
    val element = List("huy", "Nhi")
    println(Seq(Row.fromSeq(element)))
    println(productTest.map(r => Row.fromSeq(r)))
  }
  def isRegrex(str:String)= {
    val re = new Regex("[\\[\\'\\]]")
    re.findAllIn(str)
  }
}
