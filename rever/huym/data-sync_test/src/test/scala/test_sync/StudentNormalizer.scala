package test_sync

import rever.etl.data_sync.core.Normalizer

case class StudentNormalizer() extends Normalizer[StudentRecord,Map[String,Any]]{
  override def toRecord(student: StudentRecord, total: Long): Option[Map[String, Any]] = {
    val dataMap = Map[String,Any](
      StudentRecord.ID->student.id.get,
      StudentRecord.NAME->student.name.getOrElse(""),
      StudentRecord.AGE->student.age.getOrElse(0),
      StudentRecord.CLASS_NAME->student.className.getOrElse(""),
      StudentRecord.SCHOOL_NAME->student.schoolName.getOrElse("")
    )
    Some(dataMap)
  }
}
