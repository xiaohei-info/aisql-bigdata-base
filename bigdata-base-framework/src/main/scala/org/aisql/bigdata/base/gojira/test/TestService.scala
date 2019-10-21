package org.aisql.bigdata.base.gojira.test

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.aisql.bigdata.base.framework.kafka.{BaseKafkaDao, BaseKafkaService}

/**
  * Author: xiaohei
  * Date: 2019/10/21
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
class TestService extends BaseKafkaService[StreamingContext, DStream[FiGwAgrtExpressOrderEncryptBean]] {
  override protected val dao: BaseKafkaDao[StreamingContext, DStream[FiGwAgrtExpressOrderEncryptBean]] =
    new FiGwAgrtExpressOrderEncryptDao
}
