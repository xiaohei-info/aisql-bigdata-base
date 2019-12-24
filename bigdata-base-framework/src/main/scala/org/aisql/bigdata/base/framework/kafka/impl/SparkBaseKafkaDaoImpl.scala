package org.aisql.bigdata.base.framework.kafka.impl

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.aisql.bigdata.base.connector.realtime.sinks.KafkaSink
import org.aisql.bigdata.base.framework.bean.{MaxwellBean, StreamMsgBean}
import org.aisql.bigdata.base.framework.kafka.BaseKafkaDao
import org.aisql.bigdata.base.util.JavaJsonUtil
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._

/**
  * Author: xiaohei
  * Date: 2019/10/21
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
trait SparkBaseKafkaDaoImpl[B] extends BaseKafkaDao[StreamingContext, DStream[B]] {

  protected def transJson2Bean(jsonStream: DStream[String]): DStream[B]

  protected def transBean2Json(beanStream: DStream[B]): DStream[String]

  protected def transJson2BeanMaxwell(jsonStream: DStream[String]): DStream[MaxwellBean] = {
    jsonStream.map(x => JSON.parseObject(x, classOf[MaxwellBean]))
  }

  protected def tranBean2JsonMaxwell(beanStream: DStream[MaxwellBean]): DStream[String] = {
    beanStream.map(x => JavaJsonUtil.toJSONString(x))
  }

  protected def transJson2BeanStreamMsg(jsonStream: DStream[String]): DStream[StreamMsgBean] = {
    jsonStream.map(x => JSON.parseObject(x, classOf[StreamMsgBean]))
  }

  protected def tranBean2JsonStreamMsg(beanStream: DStream[StreamMsgBean]): DStream[String] = {
    beanStream.map(x => JavaJsonUtil.toJSONString(x))
  }

  override def readStream(implicit env: StreamingContext): DStream[B] = {
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> BOOTSTRAP_SERVERS,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> GROUP_ID,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    println("kafka params:")
    kafkaParams.foreach(k => println(s"${k._1}:${k._2}"))
    println(s"topic:$TOPIC")

    val jsonStream = KafkaUtils.createDirectStream[String, String](
      env,
      PreferConsistent,
      Subscribe[String, String](TOPIC.split(","), kafkaParams)
    ).map(_.value)
    transJson2Bean(jsonStream)
  }

  override def writeStream(result: DStream[B])
                          (implicit env: StreamingContext): Unit = {

    println("init kafka producer")

    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS)
        p.put("key.serializer", classOf[StringSerializer])
        p.put("value.serializer", classOf[StringSerializer])
        p
      }
      env.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    println(s"write to : $BOOTSTRAP_SERVERS, topic: $TOPIC")

    val stream = transBean2Json(result)
    stream.foreachRDD {
      rdd =>
        if (!rdd.isEmpty()) {
          rdd.foreach {
            r =>
              kafkaProducer.value.send(TOPIC, r)
          }
        }
    }
  }
}
