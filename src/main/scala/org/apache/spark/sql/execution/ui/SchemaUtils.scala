package org.apache.spark.sql.execution.ui
import com.fasterxml.jackson.databind.PropertyNamingStrategy.SnakeCaseStrategy
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe.TypeTag
/**
 * @author sunguorui
 * @date 2023年05月29日 9:29 下午
 */
object SchemaUtils {
  private lazy val strategy = new SnakeCaseStrategy

  /**
   * 构造 Schema
   *
   * @tparam T 数据类型
   * @return the Schema
   */
  def schema[T <: Product : TypeTag]: StructType = {
    withSnakeCaseName {
      ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
    }
  }


  private def withSnakeCaseName(structType: StructType): StructType = {
    StructType(structType.fields.map(translate))
  }


  // convert field name to snake-case format
  private def translate(field: StructField): StructField = {
    val newName = strategy.translate(field.name)
    field.dataType match {
      case nestedType: StructType =>
        val arr = nestedType.fields.map(f => translate(f))
        StructField(newName, StructType(arr), field.nullable, field.metadata)
      case arrayType: ArrayType =>
        val newType = arrayType.elementType match {
          case nestedType: StructType =>
            StructType(nestedType.fields.map(f => translate(f)))
          case o => o
        }
        StructField(newName, ArrayType(newType, arrayType.containsNull))
      case _ =>
        StructField(newName, field.dataType, field.nullable, field.metadata)
    }
  }

  /**
   * 获取 Schema 名称
   *
   * @param schema           the schema
   * @param withSuperiorName the super field name
   * @return the name list
   */
  def names(schema: StructType, withSuperiorName: Boolean = true): Seq[String] = {
    val list = new ListBuffer[String]()
    schema.fields.foreach(f => fieldNames(null, f, list, withSuperiorName))
    list
  }

  // ========================= private helper =========================

  private def fieldNames(superiorName: String,
                         field: StructField,
                         result: ListBuffer[String],
                         withSuperiorName: Boolean): Unit = {
    field.dataType match {
      case nestedType: StructType =>
        nestedType.fields.foreach(f => fieldNames(field.name, f, result, withSuperiorName))
      case _ =>
        if (withSuperiorName && null != superiorName) {
          result += s"$superiorName.${field.name}"
        } else {
          result += field.name
        }
    }
  }


}
