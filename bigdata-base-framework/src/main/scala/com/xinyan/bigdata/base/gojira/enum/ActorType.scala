package com.xinyan.bigdata.base.gojira.enum

/**
  * Author: xiaohei
  * Date: 2019/10/14
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
object ActorType extends Enumeration {
  type ActorType = Value
  val BEAN = Value("Bean")
  val SPARK_DAO = Value("SparkDao")
  val SPARK_SERVICE = Value("SparkService")
}
