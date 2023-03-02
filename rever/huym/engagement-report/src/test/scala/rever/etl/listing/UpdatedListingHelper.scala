package rever.etl.listing

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import rever.etl.listing.config.UpdatedListingField
import rever.etl.rsparkflow.utils.JsonUtils.{toJson, toJsonNode}

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable

object UpdatedListingHelper {
  final val enhanceUpdatedListingSchema = {
    StructType(
      Array(
        StructField(UpdatedListingField.OWNER_EMAIL, StringType, true),
        StructField(UpdatedListingField.REVER_ID, StringType, true),
        StructField(UpdatedListingField.UPDATED_TIME, LongType, true),
        StructField(UpdatedListingField.DATA, StringType, true),
        StructField(UpdatedListingField.OLD_DATA, StringType, true),
        StructField(UpdatedListingField.LISTING_ID, StringType, true)
      )
    )
  }
  final val editedInfoFieldsUdf = udf((data: String, oldData: String) => getEditedFields(data, oldData))

  def getEditedFields(data: String, oldData: String): String = {
    val dataJsonNode = toJsonNode(data)
    val oldDataJsonNode = toJsonNode(oldData)
    val editedFieldMap = mutable.Map.empty[String, Any]
    toJson(getEditedFieldsRecursion(parentKey = Seq(), editedFieldMap, dataJsonNode, oldDataJsonNode), false)
  }

  private def getEditedFieldsRecursion(
      parentKey: Seq[String],
      editedFieldMap: mutable.Map[String, Any],
      dataJsonNode: JsonNode,
      oldDataJsonNode: JsonNode
  ): mutable.Map[String, Any] = {
    val keyArray = (dataJsonNode.fieldNames().asScala ++ oldDataJsonNode.fieldNames().asScala).toSeq.distinct
    keyArray.foreach(key => {
      val dataValue = dataJsonNode.get(key)
      val oldDataValue = oldDataJsonNode.get(key)
      if (dataValue != null && oldDataValue != null && dataValue.getClass == dataValue.getClass) {
        dataValue match {
          case _: ObjectNode =>
            if (parentKey.isEmpty) editedFieldMap += (key -> mutable.Map.empty[String, Any])
            else {
              if (!editedFieldMap.exists(_._1 == key)) addEmptyMapFromParentKey(parentKey, key, editedFieldMap)
            }
            getEditedFieldsRecursion(parentKey :+ key, editedFieldMap, dataValue, oldDataValue)
          case _ =>
            if (dataValue != oldDataValue) {
              if (parentKey.isEmpty) editedFieldMap += (key -> dataValue)
              else {
                addValueFromParentKey(parentKey, key, dataValue, editedFieldMap)
              }
            }
        }
      }
    })
    editedFieldMap
  }
  def addEmptyMapFromParentKey(parentKey: Seq[String], key: String, editedFieldMap: mutable.Map[String, Any]): Unit = {
    var parentMap = editedFieldMap
    for (i <- parentKey.indices) {
      var valueElement = parentMap.get(parentKey(i))
      valueElement match {
        case Some(value: mutable.Map[String, Any]) =>
          if (i == parentKey.size - 1) {
            value.put(key, mutable.Map.empty[String, Any])
          } else {
            parentMap = value
          }
        case None =>
      }
    }
  }
  def addValueFromParentKey(
      parentKey: Seq[String],
      key: String,
      dataValue: Any,
      editedFieldMap: mutable.Map[String, Any]
  ): Unit = {
    var parentMap = editedFieldMap
    for (i <- parentKey.indices) {
      var valueElement = parentMap.get(parentKey(i))
      valueElement match {
        case Some(value: mutable.Map[String, Any]) =>
          if (i == parentKey.size - 1) {
            value.put(key, dataValue)
          } else {
            parentMap = value
          }
        case None =>
      }
    }
  }
}
