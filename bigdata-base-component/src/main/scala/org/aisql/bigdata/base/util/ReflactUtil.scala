package org.aisql.bigdata.base.util

import java.lang.reflect.{Field, Modifier}
import java.util.{Map => JMap}
import java.lang.{Iterable => JIterable}

/**
  * Author: xiaohei
  * Date: 2019/10/15
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
object ReflactUtil {

  // 注意在Scala中Map是Iterable的子类
  def isScalaMapClass(clazz: Class[_]) = {
    if (classOf[Map[_, _]].isAssignableFrom(clazz)) true else false
  }

  def isScalaIterableClass(clazz: Class[_]) = {
    if (classOf[Iterable[_]].isAssignableFrom(clazz)) true else false
  }

  def isJavaMapClass(clazz: Class[_]) = {
    if (classOf[JMap[_, _]].isAssignableFrom(clazz)) true else false
  }

  def isJavaIterableClass(clazz: Class[_]) = {
    if (classOf[JIterable[_]].isAssignableFrom(clazz)) true else false
  }

  /**
    * 根据类型获取类的字段成员属性列表
    *
    * @param clazz 类型 scala: ClassOf[T] java: T.class
    * @return 成员属性数组
    **/
  def getFields(clazz: Class[_]): Array[Field] = {
    val fields = clazz.getDeclaredFields
      .filterNot(f => Modifier.isTransient(f.getModifiers))
    fields.foreach(_.setAccessible(true))
    fields
  }
}
