package org.aisql.bigdata.base.framework.kafka.predefined

import com.alibaba.fastjson.JSON
import org.aisql.bigdata.base.framework.bean.StreamMsgBean
import org.aisql.bigdata.base.framework.kafka.impl.FlinkBaseKafkaDaoImpl
import org.aisql.bigdata.base.util.JavaJsonUtil
import org.apache.flink.streaming.api.scala._

/**
  * Author: xiaohei
  * Date: 2019/10/24
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
abstract class FlinkStreamMsgKafkaDaoImpl  extends FlinkBaseKafkaDaoImpl[StreamMsgBean] {

  override protected def transJson2Bean(jsonStream: DataStream[String]): DataStream[StreamMsgBean] = {
    jsonStream.map(x => JSON.parseObject(x, classOf[StreamMsgBean]))
  }

  override protected def transBean2Json(beanStream: DataStream[StreamMsgBean]): DataStream[String] = {
    beanStream.map(x => JavaJsonUtil.toJSONString(x))
  }
}
