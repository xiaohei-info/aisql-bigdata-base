package com.xinyan.bigdata.base.connector.realtime.sinks

import java.util

import org.redisson.Redisson
import org.redisson.config.Config
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster, JedisSentinelPool}

import scala.collection.JavaConversions._

/**
  * Author: xiaohei
  * Date: 2018/8/23
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
class RedisSink(masterName: String, hostAndPort: String) {
  lazy val sentinelPool = {

    // 数据库链接池配置
    //    val config = new JedisPoolConfig()
    //    config.setMaxTotal(100)
    //    config.setMaxIdle(50)
    //    config.setMinIdle(20)
    //    config.setMaxWaitMillis(6 * 1000)
    //    config.setTestOnBorrow(true)
    //
    //    // Redis集群的节点集合
    //    val jedisClusterNodes = new util.HashSet[HostAndPort]()
    //    hostAndPorts.split(",").foreach {
    //      x =>
    //        val hp = x.split(":")
    //        jedisClusterNodes.add(new HostAndPort(hp(0), hp(1).toInt))
    //    }
    //
    //    val jedisCluster = new JedisCluster(jedisClusterNodes, 2000, 100, config)
    //
    //
    //    sys.addShutdownHook {
    //      jedisCluster.close()
    //    }
    //    jedisCluster

    //jedis.auth("helloworld")

    val sentinels = new util.HashSet[String]()
    hostAndPort.split(",").foreach {
      x =>
        sentinels.add(x)
    }
    new JedisSentinelPool(masterName, sentinels)
  }

  lazy val cluster = {
    val nodes = new util.HashSet[HostAndPort]()
    hostAndPort.split(",").foreach {
      x =>
        val arr = x.split(":")
        nodes.add(new HostAndPort(arr(0), arr(1).toInt))
    }
    new JedisCluster(nodes)
  }

  lazy val slotAndHost = {
    val nodes = new util.HashSet[HostAndPort]()
    hostAndPort.split(",").foreach {
      x =>
        val arr = x.split(":")
        nodes.add(new HostAndPort(arr(0), arr(1).toInt))
    }
    val jedisCluster = new JedisCluster(nodes)
    val anyHostAndPortStr = jedisCluster.getClusterNodes.keySet().iterator().next()
    val slotHostMap = new util.TreeMap[Long, String]()
    val parts = anyHostAndPortStr.split(":")
    val anyHostAndPort = new HostAndPort(parts(0), Integer.parseInt(parts(1)))
    val jedis = new Jedis(anyHostAndPort.getHost, anyHostAndPort.getPort)
    val list = jedis.clusterSlots()
    list.toList.foreach {
      obj =>
        val list1 = obj.asInstanceOf[util.ArrayList[Object]]
        val master = list1.get(2).asInstanceOf[util.ArrayList[Object]]
        val hostAndPort = new String(master.get(0).asInstanceOf[Array[Byte]]) + ":" + master.get(1)
        slotHostMap.put(list1.get(0).asInstanceOf[Long], hostAndPort)
        slotHostMap.put(list1.get(1).asInstanceOf[Long], hostAndPort)
    }
    jedis.close()
    slotHostMap
  }

  lazy val redission = {
    val nodes = hostAndPort.split(",").map(x => s"redis://$x")
    val config = new Config()
    config.useClusterServers()
      // 集群状态扫描间隔时间，单位是毫秒
      .setScanInterval(2000)
      //cluster方式至少6个节点(3主3从，3主做sharding，3从用来保证主宕机后可以高可用)
      .addNodeAddress(nodes: _*)
      .setRetryInterval(1000)
      .setRetryAttempts(2)
      //连接超时5s
      .setIdleConnectionTimeout(5000)
      //ping超时500ms
      .setPingTimeout(500)

    config.setCodec(new org.redisson.client.codec.StringCodec())

    val redisson = Redisson.create(config)
    redisson
  }

  //  lazy val clusterJedises = {
  //    val nodeMap = cluster.getClusterNodes
  //    nodeMap.values().map(_.getResource)
  //
  //
  //    val anyHostAndPortStr = nodeMap.keySet().iterator().next()
  //
  //    val slotHostMap = new util.TreeMap[Long, String]()
  //    val parts = anyHostAndPortStr.split(":")
  //    val anyHostAndPort = new HostAndPort(parts(0), Integer.parseInt(parts(1)))
  //    val jedis = new Jedis(anyHostAndPort.getHost, anyHostAndPort.getPort)
  //    val list = jedis.clusterSlots()
  //    list.toList.foreach {
  //      obj =>
  //        val list1 = obj.asInstanceOf[util.ArrayList[Object]]
  //        val master = list1.get(2).asInstanceOf[util.ArrayList[Object]]
  //        val hostAndPort = new String(master.get(0).asInstanceOf[Array[Byte]]) + ":" + master.get(1)
  //        slotHostMap.put(list1.get(0).asInstanceOf[Long], hostAndPort)
  //        slotHostMap.put(list1.get(1).asInstanceOf[Long], hostAndPort)
  //    }
  //    jedis.close()
  //
  //    val slot = JedisClusterCRC16.getSlot(key)
  //    val entry = slotHostMap.lowerEntry(slot.toLong)
  //    //todo:优化
  //    nodeMap.get(entry.getValue).getResource
  //  }
}

object RedisSink {
  def apply(masterName: String, hostAndPort: String): RedisSink = new RedisSink(masterName, hostAndPort)
}
