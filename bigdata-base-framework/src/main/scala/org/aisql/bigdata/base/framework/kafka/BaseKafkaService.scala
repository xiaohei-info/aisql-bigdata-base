package org.aisql.bigdata.base.framework.kafka

import org.aisql.bigdata.base.framework.Serviceable

/**
  * Author: xiaohei
  * Date: 2019/9/19
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
trait BaseKafkaService[E, R] extends Serviceable with Serializable {

  protected val dao: BaseKafkaDao[E, R]

  def select(implicit env: E): R = {
    dao.readStream
  }

  def save(result: R)(implicit env: E): Unit = {
    dao.writeStream(result)
  }
}

