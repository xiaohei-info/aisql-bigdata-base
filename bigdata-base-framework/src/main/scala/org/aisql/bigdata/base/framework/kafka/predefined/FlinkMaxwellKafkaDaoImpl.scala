package org.aisql.bigdata.base.framework.kafka.predefined

import com.alibaba.fastjson.JSON
import org.aisql.bigdata.base.framework.bean.MaxwellBean
import org.aisql.bigdata.base.framework.kafka.impl.FlinkBaseKafkaDaoImpl
import org.aisql.bigdata.base.util.JavaJsonUtil
import org.apache.flink.streaming.api.scala._

/**
  * Author: xiaohei
  * Date: 2019/10/22
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
abstract class FlinkMaxwellKafkaDaoImpl extends FlinkBaseKafkaDaoImpl[MaxwellBean] {

  override protected def transJson2Bean(jsonStream: DataStream[String]): DataStream[MaxwellBean] = {
    jsonStream.map(x => JSON.parseObject(x, classOf[MaxwellBean]))
  }

  override protected def transBean2Json(beanStream: DataStream[MaxwellBean]): DataStream[String] = {
    beanStream.map(x => JavaJsonUtil.toJSONString(x))
  }

}
