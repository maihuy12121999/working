package rever.rsparkflow.dao

import vn.rever.jdbc.JdbcRecord

case class GenericRecord(
    primaryKeys: Seq[String],
    fields: Seq[String],
    private val dataMap: Map[String, Any] = Map.empty[String, Any]
) extends JdbcRecord {

  private val internalDataMap = scala.collection.mutable.Map.empty[String, Any]
  dataMap.foreach {
    case (k, v) =>
      internalDataMap.put(k, v)
  }

  override def getPrimaryKeys(): Seq[String] = primaryKeys

  override def getFields(): Seq[String] = fields

  override def setValues(field: String, value: Any): Unit = {
    Option(value).foreach(value => internalDataMap.put(field, value))
  }

  override def getValue(field: String): Option[Any] = {
    internalDataMap.get(field)
  }

  def toMap: Map[String, Any] = {
    getFields()
      .map(k => k -> getValue(k))
      .toMap
      .filterNot(_._2.isDefined)
      .map(e => e._1 -> e._2.get)
  }
}
