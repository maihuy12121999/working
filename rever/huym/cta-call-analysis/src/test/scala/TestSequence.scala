import org.apache.spark.sql.Row
import org.apache.spark.sql.Row.unapplySeq

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

object TestSequence {
  def main(args: Array[String]): Unit = {
    var a = List(1,2,3)
    var b = a
    b= b:+1
    println(a)
    println(b)
    val listBuffer:ListBuffer[List[Any]] = ListBuffer.empty
    listBuffer.append(List(1,2,3))
    listBuffer.append(List(2,2,3))
    listBuffer.append(List(4,2,3))
    listBuffer.append(List(6,7,8))
    listBuffer.isEmpty match {
      case true => println("Empty")
      case _ => println("Full")
    }
    val rowCallHistoryComingFirst = listBuffer.head
    println(rowCallHistoryComingFirst(1))
    val l :ListBuffer[(String,List[Any])] = ListBuffer.empty
    l.append(("SKT",List(1,2,3)))
    println(l.head._1)
    val listBufferStr:ListBuffer[String] = ListBuffer.empty
    listBufferStr.append("skt")
    listBufferStr.append("huni")
    listBufferStr.append("peanut")
    println(listBufferStr.indexOf("huni"))
    println(listBufferStr(1))
    val stringTest = ""
    val transformedString = if (stringTest!= "" && stringTest.head == '+') stringTest.substring(1) else stringTest
    println(transformedString)

  }
}
