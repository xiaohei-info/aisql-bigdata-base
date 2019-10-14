package com.xinyan.bigdata.base.gojira.enum

/**
  * Author: xiaohei
  * Date: 2019/10/14
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
object EngineType extends Enumeration {
  type EngineType = Value
  val ALL=Value("All")
  val SPARK = Value("Spark")
  val Flink = Value("Flink")
}
