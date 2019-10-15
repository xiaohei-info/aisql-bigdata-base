package org.aisql.bigdata.base.connector.realtime

import java.util.Properties

import org.aisql.bigdata.base.connector.realtime.sinks.{HBaseSink, KafkaSink, MysqlSink, RedisSink}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: xiaohei
  * Date: 2018/4/8
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
object StreamingUtil {

  def getKafkaPrdcerBroadcast(sc: SparkContext, zkHost: String): Broadcast[KafkaSink[String, String]] = {
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", zkHost)
        p.put("key.serializer", classOf[StringSerializer])
        p.put("value.serializer", classOf[StringSerializer])
        p
      }
      sc.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }
    kafkaProducer
  }

  def getHBaseConnBroadcast(sc: SparkContext, zkHost: String, confFile: String): Broadcast[HBaseSink] = {
    sc.broadcast(HBaseSink(zkHost, confFile))
  }

  def getMySqlConnBroadcast(sc: SparkContext, host: String, port: String, db: String, user: String, pwd: String): Broadcast[MysqlSink] = {
    sc.broadcast(MysqlSink(host, port, db, user, pwd))
  }

  def getMySqlConnBroadcast(sc: SparkContext): Broadcast[MysqlSink] = {
    val conf = sc.getConf
    if (checkConf(conf, "spark.mysql.host") && checkConf(conf, "spark.mysql.port")
      && checkConf(conf, "spark.mysql.db") && checkConf(conf, "spark.mysql.user")
      && checkConf(conf, "spark.mysql.pwd")) {
      val host = sc.getConf.get("spark.mysql.host")
      val port = sc.getConf.get("spark.mysql.port")
      val db = sc.getConf.get("spark.mysql.db")
      val user = sc.getConf.get("spark.mysql.user")
      val pwd = sc.getConf.get("spark.mysql.pwd")
      getMySqlConnBroadcast(sc, host, port, db, user, pwd)
    }
    else {
      getMySqlConnBroadcast(sc, "", "", "", "", "")
    }
  }

  def getRedisPoolBroadcast(sc: SparkContext, masterName: String, hostAndPort: String): Broadcast[RedisSink] = {
    sc.broadcast(RedisSink(masterName, hostAndPort))
  }

  def getRedisPoolBroadcast(sc: SparkContext): Broadcast[RedisSink] = {
    val conf = sc.getConf
    if (checkConf(conf, "spark.redis.masterName") && checkConf(conf, "spark.redis.hostAndPort")) {
      val masterName = conf.get("spark.redis.masterName")
      val hostAndPort = conf.get("spark.redis.hostAndPort")
      getRedisPoolBroadcast(sc, masterName, hostAndPort)
    }
    else {
      getRedisPoolBroadcast(sc, "", "")
    }
  }


  def getHBaseConnBroadcast(sc: SparkContext): Broadcast[HBaseSink] = {
    val conf = sc.getConf
    if (checkConf(conf, "spark.hbase.host") && checkConf(conf, "spark.hbase.config")) {
      val zkHost = conf.get("spark.hbase.host")
      val confFile = conf.get("spark.hbase.config")
      getHBaseConnBroadcast(sc, zkHost, confFile)
    } else {
      getHBaseConnBroadcast(sc, "", "")
    }
  }

  def getKafkaPrdcerBroadcast(sc: SparkContext): Broadcast[KafkaSink[String, String]] = {
    val conf = sc.getConf
    if (checkConf(conf, "spark.kafka.host")) {
      val zkHost = sc.getConf.get("spark.kafka.host")
      getKafkaPrdcerBroadcast(sc, zkHost)
    } else {
      getKafkaPrdcerBroadcast(sc, "")
    }
  }


  private def checkConf(conf: SparkConf, key: String) = {
    conf.contains(key) && conf.get(key) != "" && conf.get(key) != null
  }


}
