package org.aisql.bigdata.base.framework.kafka

import org.aisql.bigdata.base.framework.Daoable

/**
  * Author: xiaohei
  * Date: 2019/9/19
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
trait BaseKafkaDao[E, R] extends Daoable with Serializable {

  /**
    * 数据库名
    **/
  val DATABASE: String

  /**
    * 表名
    **/
  val TABLE: String

  /**
    * kafka topic
    **/
  val TOPIC: String

  /**
    * kafka 组名
    **/
  val GROUP_ID: String

  /**
    * kafka集群地址
    **/
  val BOOTSTRAP_SERVERS: String

  def readStream(implicit env: E): R

  def writeStream(result: R)(implicit env: E): Unit
}
