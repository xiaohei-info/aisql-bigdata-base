package com.xinyan.bigdata.base.framework.enums

/**
  * Author: xiaohei
  * Date: 2019/9/9
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
object TableType extends Enumeration {
  type TableTypeEnum = Value
  val TABLE = Value("table")
  val PARQUET = Value("parquet")
}
