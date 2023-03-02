package test_sync

import rever.etl.data_sync.util.Utils.ImplicitAny
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}

object StudentRecord {
  val TBL_NAME = "student"
  def field(f:String): String =f
  final val ID = field("id")
  final val NAME = field("name")
  final val AGE = field("age")
  final val CLASS_NAME = field("class_name")
  final val SCHOOL_NAME = field("school_name")

  final val primaryKeys = Seq(ID,CLASS_NAME,SCHOOL_NAME)

  final val fields = Seq(ID,NAME,AGE,CLASS_NAME,SCHOOL_NAME)
}

case class StudentRecord(
    var id:Option[Int] = None,
    var name:Option[String] = None,
    var age:Option[Int] = None,
    var className:Option[String] = None,
    var schoolName:Option[String] = None,
) extends JdbcRecord {
  override def getPrimaryKeys(): Seq[String] = StudentRecord.primaryKeys

  override def getFields(): Seq[String] = StudentRecord.fields

  override def setValues(field: String, value: Any): Unit ={
    field match {
      case StudentRecord.ID => id = value.asOpt
      case StudentRecord.NAME=>name= value.asOpt
      case StudentRecord.AGE => age = value.asOpt
      case StudentRecord.CLASS_NAME => className = value.asOpt
      case StudentRecord.SCHOOL_NAME => schoolName = value.asOpt
      case _ => throw SqlFieldMissing(field)
    }
  }

  override def getValue(field: String): Option[Any] = {
    field match {
      case StudentRecord.ID => id
      case StudentRecord.NAME=>name
      case StudentRecord.AGE => age
      case StudentRecord.CLASS_NAME => className
      case StudentRecord.SCHOOL_NAME => schoolName
      case _ => throw SqlFieldMissing(field)
    }
  }
}
