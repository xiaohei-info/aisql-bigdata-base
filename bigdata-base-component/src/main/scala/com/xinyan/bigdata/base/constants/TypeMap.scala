package com.xinyan.bigdata.base.constants

/**
  * Author: xiaohei
  * Date: 2019/10/12
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
object TypeMap {

  val hive2JavaType = Map[String, String](
    "tinyint" -> "Byte",
    "smallint" -> "Short",
    "int" -> "Int",
    "bigint" -> "Long",
    "float" -> "Float",
    "double" -> "Double",
    "decimal" -> "BigDecimal",
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

  val java2SparkdfType = Map[String, String](
    "Byte" -> "ByteType",
    "Short" -> "ShortType",
    "Int" -> "IntegerType",
    "Long" -> "LongType",
    "Float" -> "FloatType",
    "Double" -> "DoubleType",
    "BigDecimal" -> "DataTypes.createDecimalType()",
    "java.sql.Timestamp" -> "TimestampType",
    "java.sql.Date" -> "DateType",
    "String" -> "StringType",
    "Boolean" -> "BooleanType"
  )
}
