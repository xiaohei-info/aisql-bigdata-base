package com.xinyan.bigdata.base.util

import com.alibaba.fastjson.JSON
import com.xinyan.bigdata.base.constants.TypeMap
import org.apache.spark.sql.SparkSession

/**
  * Author: xiaohei
  * Date: 2019/9/15
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */

//case class TableScheme(fieldName: String, fieldType: String, fieldComment: String)

object HiveUtil {

  def getScheme(spark: SparkSession, tableName: String): Seq[(String, String, String)] = {
    val schema = spark.table(tableName).schema
    schema.map {
      t =>
        val fieldName = t.name
        val metaJson = JSON.parseObject(t.metadata.toString)
        val fieldType = TypeMap.hive2JavaType.getOrElse(metaJson.getString("HIVE_TYPE_STRING"), "String")
        val fieldComment = if (metaJson.getString("comment") == null) "" else metaJson.getString("comment")
        (fieldName, fieldType, fieldComment)
    }
  }
}
