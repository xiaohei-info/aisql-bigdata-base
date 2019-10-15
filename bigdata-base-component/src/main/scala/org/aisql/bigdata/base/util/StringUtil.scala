package org.aisql.bigdata.base.util

import java.util.UUID

import org.apache.commons.lang.StringUtils

/**
  * Author: xiaohei
  * Date: 2019/9/18
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
object StringUtil {
  def isPurifiedNum(data: Any): Boolean = {
    if (data == null) {
      return false
    }
    val dt = String.valueOf(data)
    val pattern = """([0-9]+)\.?([0-9]*)""".r
    if (pattern.findFirstIn(dt).getOrElse(None).equals(dt)) {
      return true
    }
    false
  }

  def isDecimal(data: Any): Boolean = {
    if (data == null) {
      return false
    }
    val dt = String.valueOf(data)
    val pattern = """([0-9]+)\.([0-9]+)""".r
    if (pattern.findFirstIn(dt).getOrElse(None).equals(dt)) {
      return true
    }
    false
  }

  /**
    * 生成 uuid
    *
    * @return
    */
  def getUUID: String = {
    UUID.randomUUID.toString
  }

  def getUUIDNobar: String = {
    UUID.randomUUID.toString.replaceAll("-", "")
  }

  /**
    * 判断字符串是否为32位md5格式
    *
    * @param value 初始值
    * @return
    */
  def isMd5(value: String): Boolean = {
    val pattern2 = "[0-9a-f]".r
    if (value.length != 32) {
      false
    } else if (pattern2.findAllIn(value.toLowerCase()).size != value.length) {
      false
    } else {
      true
    }
  }


  /**
    * 判断字符串是否为64位16进制格式
    *
    * @param value 初始值
    * @return
    */
  def is64Hex(value: String, length: Int): Boolean = {
    val pattern2 = "[0-9a-f]".r
    if (value.length != length) {
      false
    } else if (pattern2.findAllIn(value.toLowerCase()).size != value.length) {
      false
    } else {
      true
    }
  }

  /**
    * 业务 - 判断是否为空
    *
    * @param str
    * @return
    */
  def isNull(str: String): Boolean = {
    if ("null".equalsIgnoreCase(str) || "undefined".equals(str) || "[]".equals(str)) {
      return true
    }
    StringUtils.isBlank(str)
  }

  /**
    * 是否不为空
    *
    * @param str
    * @return
    */
  def isNotNull(str: String): Boolean = {
    !isNull(str)
  }

  private val underSeparator = "_"

  def under2camel(origin: String) = {
    val arr = origin.split(underSeparator)
    arr.map { s => s.substring(0, 1).toUpperCase + s.substring(1) }.mkString("")
  }

  def camel2under(origin: String) = {
    origin.replaceAll("([a-z])([A-Z])", "$1" + underSeparator + "$2").toLowerCase()
  }
}
