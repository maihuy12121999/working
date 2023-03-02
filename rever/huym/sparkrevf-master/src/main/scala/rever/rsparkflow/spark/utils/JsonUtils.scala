package rever.rsparkflow.spark.utils

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper, PropertyNamingStrategy}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import java.lang.reflect.{ParameterizedType, Type}

/**
  * Created by andy(anhlt)
  * */
object JsonUtils {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE)
  mapper.setSerializationInclusion(Include.NON_NULL)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  implicit class ObjectLike(val str: String) extends AnyVal {
    def asJsonObject[A: Manifest] = JsonUtils.fromJson[A](str)

    def asJsonNode = JsonUtils.toJsonNode(str)
  }

  implicit class JsonLike(val m: Any) extends AnyVal {
    def toJsonString = JsonUtils.toJson(m)
  }

  def fromJson[T: Manifest](json: String): T = {
    mapper.readValue[T](json, typeReference[T])
  }

  def toJsonNode(json: String): JsonNode =
    Option(json) match {
      case Some(s) => mapper.readTree(s)
      case _       => null
    }

  def toJson[T](t: T, pretty: Boolean = true): String = {
    if (pretty) mapper.writerWithDefaultPrettyPrinter().writeValueAsString(t)
    else mapper.writeValueAsString(t)
  }

  def readTree(json: String): JsonNode = mapper.readTree(json)

  private[this] def typeReference[T: Manifest] =
    new TypeReference[T] {
      override def getType: Type = typeFromManifest(manifest[T])
    }

  private[this] def typeFromManifest(m: Manifest[_]): Type = {
    if (m.typeArguments.isEmpty) {
      m.runtimeClass
    } else {
      new ParameterizedType {
        def getRawType = m.runtimeClass

        def getActualTypeArguments = m.typeArguments.map(typeFromManifest).toArray

        def getOwnerType = null
      }
    }
  }

  def isValidJSON(json: String): Boolean = {
    try {
      val parser = mapper.getFactory.createParser(json)
      while (parser.nextToken() != null) {}
      true
    } catch {
      case e: Exception => false
    }
  }

  def mergeJson(primaryJson: String, newJson: String): ObjectNode = {
    mergeNode(
      readTree(primaryJson).asInstanceOf[ObjectNode],
      readTree(newJson).asInstanceOf[ObjectNode]
    )
  }

  def mergeNode(primaryNode: ObjectNode, newPartialNode: ObjectNode): ObjectNode = {
    val fieldNames = newPartialNode.fieldNames()
    while (fieldNames.hasNext) {
      val fieldName: String = fieldNames.next()
      val primaryValue: JsonNode = primaryNode.get(fieldName)
      if (primaryValue == null || !primaryValue.isObject) {
        val backupValue: JsonNode = newPartialNode.get(fieldName).deepCopy()
        primaryNode.set(fieldName, backupValue)
      } else {
        val backupValue: JsonNode = newPartialNode.get(fieldName)
        if (backupValue.isObject) {
          mergeNode(primaryValue.asInstanceOf[ObjectNode], backupValue.deepCopy())
        }
      }
    }
    primaryNode
  }

}
