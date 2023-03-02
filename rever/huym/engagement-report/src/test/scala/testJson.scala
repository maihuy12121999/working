import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{LongNode, ObjectNode}
import rever.etl.rsparkflow.utils.JsonUtils.{toJson, toJsonNode}

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer
//Map(N -> 2, maihuy -> Map(b -> 2, d -> Map(faker -> 4), a -> 1, c -> 3), ch -> Map(b -> 2, a -> 1))
//("c"->6)
//vô key maihuy.d
//Map(N -> 2, maihuy -> Map(b -> 2, d -> Map(faker -> 4,c->6), a -> 1, c -> 3), ch -> Map(b -> 2, a -> 1))
object testJson {
  def main(args: Array[String]): Unit = {
    val data = "{\n  \"username\": \"1653390235296000\",\n  \"status\": 1,\n  \"created_time\": 1653390235296,\n  \"updated_time\": 1663308190648,\n  \"avatar\": \"https://portal-stag.rever.vn/user/1653390235296000/avatar\",\n  \"properties\": {\n    \"first_name\": \"Huy\",\n    \"username\": \"1653390235296000\",\n    \"full_name\": \"Nguyễn Minh Thuận\",\n    \"class\": {\"name\":\"12a2\",\n    \"age\": {\"num\":20}},\n    \"office\": \"1001\",\n    \"job_report_to\": \"1940115493331756309\",\n    \"last_name\": \"Nguyễn Minh\",\n    \"roles\": [\n      \"12\",\n      \"22\"\n    ],\n    \"work_email\": \"thuannm@rever.vn\",\n    \"user_type\": \"rv_staff\",\n    \"team\": \"1179\",\n    \"employment_status\": [\n      {\n        \"status\": \"probation\",\n        \"effective_date\": \"01/05/2022\"\n      }\n    ]\n  }\n,\"job\":{\n \"id\":1,\n \"name\":\"data engineer\"}}"
    val oldData = "{\n  \"username\": \"123\",\n  \"status\": 1,\n  \"created_time\": 16533902356,\n  \"updated_time\": 16633080648,\n  \"avatar\": \"https://portal-stag.rever.vn/user/1653390235296000/avatar\",\n  \"properties\": {\n    \"first_name\": \"Nhi\",\n    \"username\": \"1653390235296000\",\n    \"full_name\": \"Mai Huy\",\n    \"class\": {\"name\":\"12a1\",\n    \"age\": {\"num\":21}},\n    \"office\": \"1001\",\n    \"job_report_to\": \"1940115493331756309\",\n    \"last_name\": \"Nguyễn Minh\",\n    \"roles\": [\n      \"12\",\n      \"22\"\n    ],\n    \"work_email\": \"thuannm@rever.vn\",\n    \"user_type\": \"rv_staff\",\n    \"team\": \"1179\",\n    \"employment_status\": [\n      {\n        \"status\": \"probation\",\n        \"effective_date\": \"12/12/1999\"\n      }\n    ]\n  }\n,\"job\":{\n \"id\":1,\n \"name\":\"product manager\"}}"
    val dataJsonNode = toJsonNode(data)
    val oldDataJsonNode = toJsonNode(oldData)
    println(dataJsonNode.toPrettyString)
    println("==========================================================================================================")
    println(oldDataJsonNode.toPrettyString)
    var mapTest =mutable.Map("N"->2,"maihuy"->mutable.Map("a"->1,"b"->2,"d"->mutable.Map("faker"->4)),"ch"->mutable.Map("a"->1,"b"->2))
    mapTest.get("maihuy") match {
      case None=>
      case Some(maiHuyMap:mutable.Map[String,Any])=> maiHuyMap.get("d") match {
        case None =>
        case Some(d:mutable.Map[String,Any])=> d.put("c",6)
      }
      case _=>
    }
    println(mapTest)
    val result = toJson(getEditedFields(dataJsonNode,oldDataJsonNode),false)
//    println(addMapValueToMapByKey1(parentKey = None,"maihuy",("c"->3),mapTest))
    println(result)
  }
  def getEditedFields(dataJsonNode:JsonNode,oldDataJsonNode:JsonNode):mutable.Map[String,Any]={
    val editedFieldMap = mutable.Map.empty[String,Any]
    getEditedFieldsRecursion(parentKey = Seq(),editedFieldMap,dataJsonNode,oldDataJsonNode)
  }
  private def getEditedFieldsRecursion(parentKey:Seq[String],editedFieldMap:mutable.Map[String,Any],dataJsonNode:JsonNode,oldDataJsonNode:JsonNode):mutable.Map[String,Any]={
    val keyArray = (dataJsonNode.fieldNames().asScala++oldDataJsonNode.fieldNames().asScala).toSeq.distinct
    keyArray.foreach(
      key=> {
        val dataValue = dataJsonNode.get(key)
        val oldDataValue = oldDataJsonNode.get(key)
        if(dataValue!=null && oldDataValue!=null && dataValue.getClass==dataValue.getClass){
          dataValue match {
            case _: ObjectNode=>
              if(parentKey.isEmpty) editedFieldMap += (key->mutable.Map.empty[String,Any])
              else{
                if(!editedFieldMap.exists(_._1==key)) addEmptyMapFromParentKey(parentKey,key,editedFieldMap)
              }
              getEditedFieldsRecursion(parentKey:+key,editedFieldMap,dataValue,oldDataValue)
            case _:LongNode=>

            case _=>
              if(dataValue!=oldDataValue) {
                if(parentKey.isEmpty) editedFieldMap+=(key->dataValue)
                else {
                  addValueFromParentKey(parentKey,key,dataValue,editedFieldMap)
                }
              }
          }
        }
      }
    )
    editedFieldMap
  }
  def addEmptyMapFromParentKey(parentKey:Seq[String],key:String, editedFieldMap:mutable.Map[String,Any]):Unit= {
    var parentMap = editedFieldMap
    for (i <- parentKey.indices) {
      var valueElement = parentMap.get(parentKey(i))
      valueElement match {
        case Some(value: mutable.Map[String, Any]) =>
          if (i == parentKey.size - 1) {
            value.put(key,mutable.Map.empty[String,Any])
          }
          else{
            parentMap = value
          }
        case None =>
      }
    }
  }
  def addValueFromParentKey(parentKey:Seq[String],key:String, dataValue:Any, editedFieldMap:mutable.Map[String,Any]):Unit= {
    var parentMap = editedFieldMap
    for (i <- parentKey.indices) {
      var valueElement = parentMap.get(parentKey(i))
      valueElement match {
        case Some(value: mutable.Map[String, Any]) =>
          if (i == parentKey.size - 1) {
            value.put(key,dataValue)
          }
          else{
            parentMap = value
          }
        case None =>
      }
    }
  }
}
