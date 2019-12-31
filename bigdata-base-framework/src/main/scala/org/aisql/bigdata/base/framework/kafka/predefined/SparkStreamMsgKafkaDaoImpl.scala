package org.aisql.bigdata.base.framework.kafka.predefined

import com.alibaba.fastjson.JSON
import org.aisql.bigdata.base.framework.bean.StreamMsgBean
import org.aisql.bigdata.base.framework.kafka.impl.SparkBaseKafkaDaoImpl
import org.aisql.bigdata.base.util.JavaJsonUtil
import org.apache.spark.streaming.dstream.DStream

/**
  * Author: xiaohei
  * Date: 2019/10/24
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
abstract class SparkStreamMsgKafkaDaoImpl extends SparkBaseKafkaDaoImpl[StreamMsgBean] {

  override protected def transJson2Bean(jsonStream: DStream[String]): DStream[StreamMsgBean] = {
    jsonStream.flatMap {
      x =>
        try {
          Some(JSON.parseObject(x, classOf[StreamMsgBean]))
        } catch {
          case e: Exception =>
            logger.error(e.getMessage + ", json string:" + x)
            None
        }
    }
  }

  override protected def transBean2Json(beanStream: DStream[StreamMsgBean]): DStream[String] = {
    beanStream.map(x => JavaJsonUtil.toJSONString(x))
  }
}
