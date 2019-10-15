package com.xinyan.bigdata.base.framework.util

import java.lang.reflect.{GenericArrayType, Modifier, ParameterizedType, Field}
import java.lang.{Iterable => JIterable}
import java.util.{Map => JMap}

import scala.collection.JavaConversions._

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StructField, StructType, DecimalType, DataTypes}
import org.apache.spark.sql.types.DataTypes._
/**
  * Author: xiaohei
  * Date: 2019/10/15
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
object DataFrameUtil {

  /** 成员变量的类型和sparkSQL类型的映射 */
  val predefinedDataType: collection.mutable.Map[Class[_], DataType] =
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

  /** 类之间的转换。比如将java.util.Date转换为java.sql.Date */
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

  /** cache of Class -> Option[StructType] */
  private val structTypeCache = new org.apache.commons.collections.map.LRUMap(100)

  /** cache of java.lang.reflect.Type -> Option[DataType] */
  private val dataTypeCache = new org.apache.commons.collections.map.LRUMap(1000)

  /** cache of Class -> Array[Field] */
  private val classFieldsCache = collection.mutable.Map[Class[_], Array[Field]]()

  /** scala.collection.Map 类型的Class的cache */
  private val scalaMapClassCache = collection.mutable.Set[Class[_]]()

  /** scala.collection.Iterable 类型的Class的cache */
  private val scalaIterableClassCache = collection.mutable.Set[Class[_]]()

  /** java.util.Map 类型的Class的cache */
  private val javaMapClassCache = collection.mutable.Set[Class[_]]()

  /** java.lang.Iterable 类型的Class的cache */
  private val javaIterableClassCache = collection.mutable.Set[Class[_]]()

  // 注意在Scala中Map是Iterable的子类
  def isScalaMapClass(clazz: Class[_]) = {
    if (scalaMapClassCache.contains(clazz)) true
    else if (classOf[Map[_, _]].isAssignableFrom(clazz)) {
      scalaMapClassCache += clazz
      true
    } else false
  }

  def isScalaIterableClass(clazz: Class[_]) = {
    if (scalaIterableClassCache.contains(clazz)) true
    else if (classOf[Iterable[_]].isAssignableFrom(clazz)) {
      scalaIterableClassCache += clazz
      true
    } else false
  }

  def isJavaMapClass(clazz: Class[_]) = {
    if (javaMapClassCache.contains(clazz)) true
    else if (classOf[JMap[_, _]].isAssignableFrom(clazz)) {
      javaMapClassCache += clazz
      true
    } else false
  }

  def isJavaIterableClass(clazz: Class[_]) = {
    if (javaIterableClassCache.contains(clazz)) true
    else if (classOf[JIterable[_]].isAssignableFrom(clazz)) {
      javaIterableClassCache += clazz
      true
    } else false
  }

  def getFields(clazz: Class[_]) =
    classFieldsCache.getOrElseUpdate(clazz, {
      val fields = clazz.getDeclaredFields
        .filterNot(f => Modifier.isTransient(f.getModifiers))
        .flatMap(f =>
          getDataType(f.getGenericType) match {
            case Some(_) => Some(f)
            case None => None
          })
      fields.foreach(_.setAccessible(true))
      fields
    })

  /**
    * 根据Class对象，生成StructType对象。
    */
  def getStructType(clazz: Class[_]): Option[StructType] = {
    val cachedStructType = structTypeCache.get(clazz)
    if (cachedStructType == null) {
      val fields = getFields(clazz)
      val newStructType =
        if (fields.isEmpty) None
        else {
          val types = fields.map(f => {
            val dataType = getDataType(f.getGenericType).get
            StructField(f.getName, dataType, nullable = true) // 默认所有的字段都可能为空
          })
          if (types.isEmpty) None else Some(StructType(types))
        }
      structTypeCache.put(clazz, newStructType)
      newStructType
    } else cachedStructType.asInstanceOf[Option[StructType]]
  }

  /**
    * 根据java.lang.reflect.Type获取org.apache.spark.sql.types.DataType
    * 递归处理嵌套类型
    */
  private def getDataType(tp: java.lang.reflect.Type): Option[DataType] = {
    val cachedDataType = dataTypeCache.get(tp)
    if (cachedDataType == null) {
      val newDataType = tp match {
        case ptp: ParameterizedType => // 带有泛型的数据类型,e.g. List[String]
          val clazz = ptp.getRawType.asInstanceOf[Class[_]]
          val rowTypes = ptp.getActualTypeArguments
          if (isScalaMapClass(clazz) || isJavaMapClass(clazz)) {
            (getDataType(rowTypes(0)), getDataType(rowTypes(1))) match {
              case (Some(keyType), Some(valueType)) =>
                Some(DataTypes.createMapType(keyType, valueType, true))
              case _ => None
            }
          } else if (isScalaIterableClass(clazz) || isJavaIterableClass(clazz)) {
            getDataType(rowTypes(0)) match {
              case Some(dataType) => Some(DataTypes.createArrayType(dataType, true))
              case None => None
            }
          } else {
            getStructType(clazz)
          }
        case gatp: GenericArrayType => // 泛型数据类型的数组,e.g. Array[List[String]]
          getDataType(gatp.getGenericComponentType) match {
            case Some(dataType) => Some(DataTypes.createArrayType(dataType, true))
            case None => None
          }
        case clazz: Class[_] => // 没有泛型的类型（包括没有指定泛型的Map和Collection）
          predefinedDataType.get(clazz) match {
            case Some(tp) => Some(tp)
            case None =>
              if (clazz.isArray) {
                // 非泛型对象的数组
                getDataType(clazz.getComponentType) match {
                  case Some(dataType) => Some(DataTypes.createArrayType(dataType, true))
                  case None => None
                }
              } else if (isScalaMapClass(clazz) || isJavaMapClass(clazz)) {
                Some(DataTypes.createMapType(StringType, StringType, true))
              } else if (isScalaIterableClass(clazz) || isJavaIterableClass(clazz)) {
                Some(DataTypes.createArrayType(StringType, true))
              } else {
                // 一般Object类型，转换为嵌套类型
                getStructType(clazz)
              }
          }
        case _ =>
          throw new IllegalArgumentException("不支持 WildcardType 和 TypeVariable")
      }
      dataTypeCache.put(tp, newDataType)
      newDataType
    } else cachedDataType.asInstanceOf[Option[DataType]]
  }

  /**
    * 读取一行数据
    */
  def getRow(clazz: Class[_], obj: Any): Option[Row] =
    getStructType(clazz) match {
      case Some(_) =>
        if (obj == null) Some(null)
        else Some(Row(getFields(clazz).flatMap(f => getCell(f.getGenericType, f.get(obj))): _*))
      case None => None
    }

  /**
    * 读取单个数据
    */
  private def getCell(tp: java.lang.reflect.Type, value: Any): Option[Any] =
    tp match {
      case ptp: ParameterizedType => // 带有泛型的数据类型,e.g. List[String]
        val clazz = ptp.getRawType.asInstanceOf[Class[_]]
        val rowTypes = ptp.getActualTypeArguments
        if (isScalaMapClass(clazz)) {
          (getDataType(rowTypes(0)), getDataType(rowTypes(1))) match {
            case (Some(keyType), Some(valueType)) =>
              if (value == null) Some(null)
              else Some(value.asInstanceOf[Map[Any, Any]].filterKeys(_ != null)
                .map { case (k, v) => getCell(rowTypes(0), k).get -> getCell(rowTypes(1), v).get })
            case _ => None
          }
        } else if (isScalaIterableClass(clazz)) {
          getDataType(rowTypes(0)) match {
            case Some(_) =>
              if (value == null) Some(null)
              else Some(value.asInstanceOf[Iterable[Any]].filter(_ != null).map(v => getCell(rowTypes(0), v).get).toSeq)
            case None => None
          }
        } else if (isJavaIterableClass(clazz)) {
          getDataType(rowTypes(0)) match {
            case Some(_) =>
              if (value == null) Some(null)
              else Some(value.asInstanceOf[JIterable[Any]].filter(_ != null).map(v => getCell(rowTypes(0), v).get).toSeq)
            case None => None
          }
        } else if (isJavaMapClass(clazz)) {
          (getDataType(rowTypes(0)), getDataType(rowTypes(1))) match {
            case (Some(keyType), Some(valueType)) =>
              if (value == null) Some(null)
              else Some(value.asInstanceOf[JMap[Any, Any]].filterKeys(_ != null)
                .map { case (k, v) => getCell(rowTypes(0), k).get -> getCell(rowTypes(1), v).get })
            case _ => None
          }
        } else {
          getCell(clazz, value)
        }
      case gatp: GenericArrayType => // 泛型数据类型的数组,e.g. Array[List[String]]
        getDataType(gatp.getGenericComponentType) match {
          case Some(dataType) => Some(value.asInstanceOf[Array[Any]].map(v => getCell(gatp.getGenericComponentType, v).get).toSeq)
          case None => None
        }
      case clazz: Class[_] => // 没有泛型的类型（包括没有指定泛型的Map和Collection）
        predefinedDataType.get(clazz) match {
          case Some(_) =>
            classConverter.get(clazz) match {
              case Some(converter) => Some(if (value == null) null else converter(value))
              case None => Some(value)
            }
          case None =>
            if (clazz.isArray) {
              // 非泛型对象的数组
              getDataType(clazz.getComponentType) match {
                case Some(dataType) =>
                  if (value == null) Some(null)
                  else Some(value.asInstanceOf[Array[_]].filter(_ != null).flatMap(v => getCell(clazz.getComponentType, v)).toSeq)
                case None => None
              }
            } else if (isScalaMapClass(clazz)) {
              Some(value.asInstanceOf[Map[Any, Any]].filterKeys(_ != null)
                .map { case (k, v) => getCell(classOf[Any], k).get -> getCell(classOf[Any], v).get })
            } else if (isScalaIterableClass(clazz)) {
              Some(value.asInstanceOf[Iterable[Any]].filter(_ != null)
                .map(v => getCell(classOf[Any], v).get).toSeq)
            } else if (isJavaIterableClass(clazz)) {
              Some(value.asInstanceOf[JIterable[Any]].filter(_ != null)
                .map(v => getCell(classOf[Any], v).get).toSeq)
            } else if (isJavaMapClass(clazz)) {
              Some(value.asInstanceOf[JMap[Any, Any]].filterKeys(_ != null)
                .map { case (k, v) => getCell(classOf[Any], k).get -> getCell(classOf[Any], v).get })
            } else {
              // 一般Object类型，转换为嵌套类型
              getRow(clazz, value)
            }
        }
      case _ =>
        throw new IllegalArgumentException("不支持 WildcardType 和 TypeVariable")
    }

}
