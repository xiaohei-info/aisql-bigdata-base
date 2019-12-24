package org.aisql.bigdata.base.framework.kafka.impl

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.aisql.bigdata.base.framework.bean.{MaxwellBean, StreamMsgBean}
import org.aisql.bigdata.base.framework.kafka.BaseKafkaDao
import org.aisql.bigdata.base.util.JavaJsonUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

/**
  * Author: xiaohei
  * Date: 2019/8/29
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
trait FlinkBaseKafkaDaoImpl[B] extends BaseKafkaDao[StreamExecutionEnvironment, DataStream[B]] {

  protected def transJson2Bean(jsonStream: DataStream[String]): DataStream[B]

  protected def transBean2Json(beanStream: DataStream[B]): DataStream[String]

  protected def transJson2BeanMaxwell(jsonStream: DataStream[String]): DataStream[MaxwellBean] = {
    jsonStream.map(x => JSON.parseObject(x, classOf[MaxwellBean]))
  }

  protected def tranBean2JsonMaxwell(beanStream: DataStream[MaxwellBean]): DataStream[String] = {
    beanStream.map(x => JavaJsonUtil.toJSONString(x))
  }

  protected def transJson2BeanStreamMsg(jsonStream: DataStream[String]): DataStream[StreamMsgBean] = {
    jsonStream.map(x => JSON.parseObject(x, classOf[StreamMsgBean]))
  }

  protected def tranBean2JsonStreamMsg(beanStream: DataStream[StreamMsgBean]): DataStream[String] = {
    beanStream.map(x => JavaJsonUtil.toJSONString(x))
  }

  override def readStream(implicit env: StreamExecutionEnvironment): DataStream[B] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS)
    properties.setProperty("group.id", GROUP_ID)

    //flink.partition-discovery.interval-millis动态分区发现
    val consumer = new FlinkKafkaConsumer[String](TOPIC, new SimpleStringSchema(), properties)
    val jsonStream = env.addSource(consumer)
    transJson2Bean(jsonStream)
  }

  override def writeStream(result: DataStream[B])
                          (implicit env: StreamExecutionEnvironment): Unit = {
    val producer = new FlinkKafkaProducer[String](BOOTSTRAP_SERVERS, GROUP_ID, new SimpleStringSchema)
    producer.setWriteTimestampToKafka(true)
    transBean2Json(result).addSink(producer)
  }
}
