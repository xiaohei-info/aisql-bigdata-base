package org.aisql.bigdata.base.gojira.test

import com.alibaba.fastjson.JSON
import org.aisql.bigdata.base.framework.kafka.impl.SparkBaseKafkaDaoImpl
import org.aisql.bigdata.base.util.JavaJsonUtil
import org.apache.spark.streaming.dstream.DStream

/**
  * Author: xiaohei
  * Date: 2019/10/21
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
class FiGwAgrtExpressOrderEncryptDao extends SparkBaseKafkaDaoImpl[FiGwAgrtExpressOrderEncryptBean] {

  override protected def transJson2Bean(jsonStream: DStream[String]): DStream[FiGwAgrtExpressOrderEncryptBean] = {
    jsonStream.map(x => JSON.parseObject(x, classOf[FiGwAgrtExpressOrderEncryptBean]))
  }

  override protected def transBean2Json(beanStream: DStream[FiGwAgrtExpressOrderEncryptBean]): DStream[String] = {
    beanStream.map(x => JavaJsonUtil.toJSONString(x))
  }

  override val BOOTSTRAP_SERVERS: String = _
  override val DATABASE: String = ""
  override val ZK_HOST: String = _
  override val GROUP_ID: String =""
  override val TABLE: String = ""
  override val TOPIC: String = ""
}
