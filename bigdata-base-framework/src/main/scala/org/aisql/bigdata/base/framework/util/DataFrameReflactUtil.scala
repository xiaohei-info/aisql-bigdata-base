package org.aisql.bigdata.base.framework.util

import java.lang.reflect.{Field, GenericArrayType, ParameterizedType}
import java.lang.{Iterable => JIterable}
import java.util.{Map => JMap}

import org.aisql.bigdata.base.util.ReflactUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{BinaryType => _, BooleanType => _, ByteType => _, CalendarIntervalType => _, DateType => _, DoubleType => _, FloatType => _, IntegerType => _, LongType => _, ShortType => _, StringType => _, TimestampType => _, _}

import scala.collection.JavaConversions._

/**
  * Author: xiaohei
  * Date: 2019/10/15
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
object DataFrameReflactUtil {

  /** cache of Class -> Option[StructType] */
  private val structTypeCache = new org.apache.commons.collections.map.LRUMap(100)

  /** cache of java.lang.reflect.Type -> Option[DataType] */
  private val dataTypeCache = new org.apache.commons.collections.map.LRUMap(1000)

  /** cache of Class -> Array[Field] */
  private val classFieldsCache = collection.mutable.Map[Class[_], Array[Field]]()

  /**
    * java/scala 类型对应 spark.sql 的 DataType 类型
    **/
  private val preDefinedDataType: collection.mutable.Map[Class[_], DataType] =
    collection.mutable.Map(
      (classOf[Boolean], BooleanType),
      (classOf[java.lang.Boolean], BooleanType),
      (classOf[Byte], ByteType),
      (classOf[java.lang.Byte], ByteType),
      (classOf[Array[Byte]], BinaryType),
      (classOf[Array[java.lang.Byte]], BinaryType),
      (classOf[Short], ShortType),
      (classOf[java.lang.Short], ShortType),
      (classOf[Int], IntegerType),
      (classOf[java.lang.Integer], IntegerType),
      (classOf[Long], LongType),
      (classOf[java.lang.Long], LongType),
      (classOf[Float], FloatType),
      (classOf[java.lang.Float], FloatType),
      (classOf[Double], DoubleType),
      (classOf[java.lang.Double], DoubleType),
      (classOf[Char], StringType),
      (classOf[java.lang.Character], StringType),
      (classOf[Array[Char]], StringType),
      (classOf[Array[java.lang.Character]], StringType),
      (classOf[String], StringType),
      (classOf[java.math.BigDecimal], DecimalType.SYSTEM_DEFAULT),
      (classOf[java.util.Date], DateType),
      (classOf[java.sql.Date], DateType),
      (classOf[java.security.Timestamp], TimestampType),
      (classOf[java.sql.Timestamp], TimestampType),
      (classOf[java.util.Calendar], CalendarIntervalType),
      // 成员为Object类型的，都转为String
      (classOf[Any], StringType))

  /**
    * 小众数据类型转换,如 Char -> String
    * 根据数据类型获取对应结果类型的转换函数
    **/
  private val classConverter: Map[Class[_], (Any) => _ <: Any] =
    Map(
      classOf[java.util.Date] ->
        ((o: Any) => new java.sql.Date(o.asInstanceOf[java.util.Date].getTime)),
      classOf[Char] ->
        ((o: Any) => o.asInstanceOf[Char].toString),
      classOf[java.lang.Character] ->
        ((o: Any) => o.asInstanceOf[java.lang.Character].toString),
      classOf[Array[Char]] ->
        ((o: Any) => new String(o.asInstanceOf[Array[Char]])),
      classOf[Array[java.lang.Character]] ->
        ((o: Any) => new String(o.asInstanceOf[Array[java.lang.Character]].map(_.charValue))),
      classOf[Any] ->
        ((o: Any) => o.toString))

  /**
    * 根据Class类型，生成StructType对象
    */
  def getStructType(clazz: Class[_]): Option[StructType] = {
    val cachedStructType = structTypeCache.get(clazz)
    if (cachedStructType == null) {
      //获取Class类型的所有成员变量
      val fields = ReflactUtil.getFields(clazz)
      //依次转化为DataType,转化失败则为空
      val dataTypes = fields.map(f => (f.getName, switchDataType(f.getGenericType))).filter(_._2.nonEmpty).map(f => (f._1, f._2.get))
      //转化成功后创建StructField
      val structFields = dataTypes.map {
        case (fieldName, dataType) =>
          // 默认所有的字段都可能为空
          StructField(fieldName, dataType, nullable = true)
      }
      val newStructType = if (structFields.nonEmpty) Some(StructType(structFields)) else None
      structTypeCache.put(clazz, newStructType)
      newStructType
    } else {
      cachedStructType.asInstanceOf[Option[StructType]]
    }
  }

  /**
    * 根据Class类型与obj对象数据,生成Row对象
    *
    * @param clazz 对象类型
    * @param obj   携带数据的对象
    * @return Row中存储Any类型可以直接匹配到StructType的schema中
    */
  def generateRowValue(clazz: Class[_], obj: Any): Option[Row] = {
    //获取可转换字段
    val fields = getUsefulFields(clazz)
    if (fields.isEmpty) return None

    //值为null时也要返回null
    if (obj == null) {
      Some(null)
    }
    else {
      Some(Row(fields.flatMap(f => getCell(f.getGenericType, f.get(obj))): _*))
    }
  }

  def generatePojoValue(clazz: Class[_], row: Row): Any = {
    val bean = clazz.newInstance()
    val fields = getUsefulFields(clazz).map(f => (f.getName, f)).toMap
    row.schema.foreach {
      s =>
        fields.get(s.name).foreach {
          f =>
            f.setAccessible(true)
            f.set(bean, row.get(row.fieldIndex(s.name)))
        }
    }
    bean
  }

  /**
    * 根据 java.lang.reflect.Type 获取 org.apache.spark.sql.types.DataType
    * 递归处理嵌套类型
    */
  private def switchDataType(javaType: java.lang.reflect.Type): Option[DataType] = {
    val cachedDataType = dataTypeCache.get(javaType)
    if (cachedDataType == null) {
      val newDataType = javaType match {
        // 带有泛型的数据类型,e.g. List[String]
        case ptp: ParameterizedType =>
          val clazz = ptp.getRawType.asInstanceOf[Class[_]]
          val rowTypes = ptp.getActualTypeArguments
          if (ReflactUtil.isScalaMapClass(clazz) || ReflactUtil.isJavaMapClass(clazz)) {
            (switchDataType(rowTypes(0)), switchDataType(rowTypes(1))) match {
              case (Some(keyType), Some(valueType)) =>
                Some(DataTypes.createMapType(keyType, valueType, true))
              case _ => None
            }
          } else if (ReflactUtil.isScalaIterableClass(clazz) || ReflactUtil.isJavaIterableClass(clazz)) {
            switchDataType(rowTypes(0)) match {
              case Some(dataType) => Some(DataTypes.createArrayType(dataType, true))
              case None => None
            }
          } else {
            //聚合类型,其中还有子字段
            getStructType(clazz)
          }
        // 泛型数据类型的数组,e.g. Array[List[String]]
        case gatp: GenericArrayType =>
          switchDataType(gatp.getGenericComponentType) match {
            case Some(dataType) => Some(DataTypes.createArrayType(dataType, true))
            case None => None
          }
        // 没有泛型的类型（包括没有指定泛型的Map和Collection）
        case clazz: Class[_] =>
          preDefinedDataType.get(clazz) match {
            case Some(tp) => Some(tp)
            case None =>
              if (clazz.isArray) {
                // 非泛型对象的数组
                switchDataType(clazz.getComponentType) match {
                  case Some(dataType) => Some(DataTypes.createArrayType(dataType, true))
                  case None => None
                }
              } else if (ReflactUtil.isScalaMapClass(clazz) || ReflactUtil.isJavaMapClass(clazz)) {
                Some(DataTypes.createMapType(StringType, StringType, true))
              } else if (ReflactUtil.isScalaIterableClass(clazz) || ReflactUtil.isJavaIterableClass(clazz)) {
                Some(DataTypes.createArrayType(StringType, true))
              } else {
                // 一般Object类型，转换为嵌套类型
                getStructType(clazz)
              }
          }
        case _ =>
          throw new IllegalArgumentException("不支持 WildcardType 和 TypeVariable")
      }
      dataTypeCache.put(javaType, newDataType)
      newDataType
    } else {
      cachedDataType.asInstanceOf[Option[DataType]]
    }
  }


  /**
    * 根据Class类型获取可用的成员变量(可被转换为DataType类型)
    * Class类型中所有的成员变量将会逐一尝试转换为DataType
    **/
  private def getUsefulFields(clazz: Class[_]): Array[Field] = {
    classFieldsCache.getOrElseUpdate(clazz, {
      val fields = ReflactUtil.getFields(clazz)
      fields.filter(f => switchDataType(f.getGenericType).nonEmpty)
    })
  }


  /**
    * 根据字段类型和字段值,转换为Row中的Cell
    *
    * @param tp    字段类型
    * @param value 字段值
    * @return Cell单元格值
    */
  private def getCell(tp: java.lang.reflect.Type, value: Any): Option[Any] =
    tp match {
      // 带有泛型的数据类型,e.g. List[String]
      case ptp: ParameterizedType =>
        val clazz = ptp.getRawType.asInstanceOf[Class[_]]
        val rowTypes = ptp.getActualTypeArguments
        if (ReflactUtil.isScalaMapClass(clazz)) {
          (switchDataType(rowTypes(0)), switchDataType(rowTypes(1))) match {
            case (Some(keyType), Some(valueType)) =>
              if (value == null) Some(null)
              else Some(value.asInstanceOf[Map[Any, Any]].filterKeys(_ != null)
                .map { case (k, v) => getCell(rowTypes(0), k).get -> getCell(rowTypes(1), v).get })
            case _ => None
          }
        } else if (ReflactUtil.isScalaIterableClass(clazz)) {
          switchDataType(rowTypes(0)) match {
            case Some(_) =>
              if (value == null) Some(null)
              else Some(value.asInstanceOf[Iterable[Any]].filter(_ != null).map(v => getCell(rowTypes(0), v).get).toSeq)
            case None => None
          }
        } else if (ReflactUtil.isJavaIterableClass(clazz)) {
          switchDataType(rowTypes(0)) match {
            case Some(_) =>
              if (value == null) Some(null)
              else Some(value.asInstanceOf[JIterable[Any]].filter(_ != null).map(v => getCell(rowTypes(0), v).get).toSeq)
            case None => None
          }
        } else if (ReflactUtil.isJavaMapClass(clazz)) {
          (switchDataType(rowTypes(0)), switchDataType(rowTypes(1))) match {
            case (Some(keyType), Some(valueType)) =>
              if (value == null) Some(null)
              else Some(value.asInstanceOf[JMap[Any, Any]].filterKeys(_ != null)
                .map { case (k, v) => getCell(rowTypes(0), k).get -> getCell(rowTypes(1), v).get })
            case _ => None
          }
        } else {
          getCell(clazz, value)
        }
      // 泛型数据类型的数组,e.g. Array[List[String]]
      case gatp: GenericArrayType =>
        switchDataType(gatp.getGenericComponentType) match {
          case Some(dataType) => Some(value.asInstanceOf[Array[Any]].map(v => getCell(gatp.getGenericComponentType, v).get).toSeq)
          case None => None
        }
      // 没有泛型的类型（包括没有指定泛型的Map和Collection）
      case clazz: Class[_] =>
        preDefinedDataType.get(clazz) match {
          case Some(_) =>
            classConverter.get(clazz) match {
              case Some(converter) => Some(if (value == null) null else converter(value))
              case None => Some(value)
            }
          case None =>
            if (clazz.isArray) {
              // 非泛型对象的数组
              switchDataType(clazz.getComponentType) match {
                case Some(dataType) =>
                  if (value == null) Some(null)
                  else Some(value.asInstanceOf[Array[_]].filter(_ != null).flatMap(v => getCell(clazz.getComponentType, v)).toSeq)
                case None => None
              }
            } else if (ReflactUtil.isScalaMapClass(clazz)) {
              Some(value.asInstanceOf[Map[Any, Any]].filterKeys(_ != null)
                .map { case (k, v) => getCell(classOf[Any], k).get -> getCell(classOf[Any], v).get })
            } else if (ReflactUtil.isScalaIterableClass(clazz)) {
              Some(value.asInstanceOf[Iterable[Any]].filter(_ != null)
                .map(v => getCell(classOf[Any], v).get).toSeq)
            } else if (ReflactUtil.isJavaIterableClass(clazz)) {
              Some(value.asInstanceOf[JIterable[Any]].filter(_ != null)
                .map(v => getCell(classOf[Any], v).get).toSeq)
            } else if (ReflactUtil.isJavaMapClass(clazz)) {
              Some(value.asInstanceOf[JMap[Any, Any]].filterKeys(_ != null)
                .map { case (k, v) => getCell(classOf[Any], k).get -> getCell(classOf[Any], v).get })
            } else {
              // 一般Object类型，转换为嵌套类型
              generateRowValue(clazz, value)
            }
        }
      case _ =>
        throw new IllegalArgumentException("不支持 WildcardType 和 TypeVariable")
    }

}
