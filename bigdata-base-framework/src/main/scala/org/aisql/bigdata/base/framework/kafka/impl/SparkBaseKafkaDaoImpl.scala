package org.aisql.bigdata.base.framework.kafka.impl

import java.util.Properties

import org.aisql.bigdata.base.connector.realtime.sinks.KafkaSink
import org.aisql.bigdata.base.framework.kafka.BaseKafkaDao
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

  override def readStream(implicit env: StreamingContext): DStream[B] = {
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> BOOTSTRAP_SERVERS,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> GROUP_ID,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    val jsonStream = KafkaUtils.createDirectStream[String, String](
      env,
      PreferConsistent,
      Subscribe[String, String](TOPIC.split(","), kafkaParams)
    ).map(_.value)
    transJson2Bean(jsonStream)
  }

  override def writeStream(result: DStream[B])
                          (implicit env: StreamingContext): Unit = {
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", ZK_HOST)
        p.put("key.serializer", classOf[StringSerializer])
        p.put("value.serializer", classOf[StringSerializer])
        p
      }
      env.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }
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
