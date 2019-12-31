package org.aisql.bigdata.base.framework.kafka.predefined

import com.alibaba.fastjson.JSON
import org.aisql.bigdata.base.framework.bean.MaxwellBean
import org.aisql.bigdata.base.framework.kafka.impl.SparkBaseKafkaDaoImpl
import org.aisql.bigdata.base.util.JavaJsonUtil
import org.apache.spark.streaming.dstream.DStream

/**
  * Author: xiaohei
  * Date: 2019/10/22
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
abstract class SparkMaxwellKafkaDaoImpl extends SparkBaseKafkaDaoImpl[MaxwellBean] {

  override protected def transJson2Bean(jsonStream: DStream[String]): DStream[MaxwellBean] = {
    jsonStream.flatMap {
      x =>
        try {
          Some(JSON.parseObject(x, classOf[MaxwellBean]))
        } catch {
          case e: Exception =>
            logger.error(e.getMessage + ", json string:" + x)
            None
        }
    }
  }

  override protected def transBean2Json(beanStream: DStream[MaxwellBean]): DStream[String] = {
    beanStream.map(x => JavaJsonUtil.toJSONString(x))
  }

}
