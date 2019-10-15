package com.xinyan.bigdata.base.constants

/**
  * Author: xiaohei
  * Date: 2019/10/12
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
object TypeMap {

  val hive2JavaType = Map[String, String](
    "tinyint" -> "java.lang.Byte",
    "smallint" -> "Short",
    "int" -> "Int",
    "bigint" -> "java.lang.Long",
    "float" -> "Float",
    "double" -> "Double",
    "decimal" -> "java.math.BigDecimal",
    "timestamp" -> "java.sql.Timestamp",
    "date" -> "java.sql.Date",
    //?
    "interval" -> "Int",
    "string" -> "String",
    "varchar" -> "String",
    "char" -> "String",
    "boolean" -> "Boolean",
    //?
    "binary" -> "String"
  )
}
