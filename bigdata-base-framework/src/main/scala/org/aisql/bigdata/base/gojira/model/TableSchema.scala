package org.aisql.bigdata.base.gojira.model

/**
  * Author: xiaohei
  * Date: 2019/10/16
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
case class FieldMeta(ame: String, javaType: String, comment: String)

case class TableSchema(tableName: String, baseClass: String, fieldsMeta: Seq[FieldMeta], isMaxwell: Boolean = false)
