package com.xinyan.bigdata.base.connector.realtime.sinks

import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}


/**
  * Author: xiaohei
  * Date: 2018/4/8
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {
  lazy val producer = createProducer()

  def send(topic: String, key: K, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, key, value))

  def send(topic: String, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, value))

  def flush() = producer.flush()
}

object KafkaSink {

  def apply[K, V](config: java.util.Properties): KafkaSink[K, V] = {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[K, V](config)
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new KafkaSink[K, V](createProducerFunc)
  }

}
