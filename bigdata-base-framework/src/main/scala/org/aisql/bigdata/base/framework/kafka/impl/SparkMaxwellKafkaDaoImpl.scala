package org.aisql.bigdata.base.framework.kafka.impl

import com.alibaba.fastjson.JSON
import org.aisql.bigdata.base.framework.bean.MaxwellBean
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
    jsonStream.map(x => JSON.parseObject(x, classOf[MaxwellBean]))
  }

  override protected def transBean2Json(beanStream: DStream[MaxwellBean]): DStream[String] = {
    beanStream.map(x => JavaJsonUtil.toJSONString(x))
  }

}
