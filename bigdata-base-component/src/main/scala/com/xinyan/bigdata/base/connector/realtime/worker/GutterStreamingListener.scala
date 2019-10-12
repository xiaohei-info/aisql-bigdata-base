package com.xinyan.bigdata.base.connector.realtime.worker

import com.alibaba.fastjson.JSONObject
import com.xinyan.bigdata.base.connector.realtime.sinks.KafkaSink
import com.xinyan.bigdata.base.util.DateUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchStarted}

/***
  * Author: zhiyi
  * Date: 2018/9/3
  * Email: zhiyi_yao@xinyan.com
  *
  * @param delayAlarmTime 延时阈值（单位为分钟
  * @param streamAppName 实时流app名字
  * @param producer kafka producer
  */
class GutterStreamingListener(delayAlarmTime: Long, streamAppName: String, producer: Broadcast[KafkaSink[String, String]]) extends StreamingListener {
  // Batch启动
  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
    //单位毫秒
    val schedulingDelay = batchStarted.batchInfo.schedulingDelay.getOrElse(0L)
    // input size
    val inputSize = batchStarted.batchInfo.numRecords
    //延迟时长（单位分钟，用以短信告警
    val schedulingDelayFloor2Minutes: Double = Math.floor((schedulingDelay / 60000).toDouble)
    val batchTime: Time = batchStarted.batchInfo.batchTime
    //yyyy-MM-dd HH:mm:ss
    val batchTimeFormat = DateUtil.ts2date(batchTime.milliseconds.toString)
    //延迟超过阈值的时间（毫秒）
    val overtime = schedulingDelay - delayAlarmTime*60*1000
    //是否延迟
    val isOvertime = if (overtime>0) true else false

    val json = new JSONObject
    json.put("schedulingDelay",schedulingDelay)
    json.put("type","stream-bigdata-monitor")
    json.put("batchTime",batchTimeFormat)
    json.put("streamAppName",streamAppName)
    json.put("delayAlarmTime",delayAlarmTime)
    json.put("inputSize",inputSize)
    json.put("isOvertime",isOvertime)
    json.put("overtime",overtime)
    json.put("schedulingDelayFloor2Minutes",schedulingDelayFloor2Minutes)

    producer.value.send("CREDIT-LOGPROCESSOR-ROCORD-STREAM-MONITOR-LOG",json.toJSONString)



    //短信效果 --->  【CDH81-60】实时流(stream-gutter-basic-dev)延时告警：时间段(2018-08-31 15:05:43 ~ 2018-09-03 15:05:43)， 延迟批次数(1,251)次，最近一次延时(0)分钟,阈值(10)分钟



    //    //微信告警
    //    if (schedulingDelay - alertTime > 0 && accum.value < 4) {
    //      NetUtil.wechatAlert(s"【CDH81-60】基础数据实时流($channel)延迟告警第 ${accum.value} 次： 批次为（$batchTimeFormat）" +
    //        s"延时（$schedulingDelayFloor2Minutes）秒,阈值（$alertTime）秒")
    //      println("=== wecharlog ===")
    //     println(s"【CDH81-60】基础数据实时流($streamAppName) 时间：${20180831 16:00} - ${20180831 16:10}， 延迟批次 ${xxx}，最近一次延时（$schedulingDelayFloor2Minutes）分钟,阈值（$delayAlarmTime）秒")
    //      accum.add(1L)
    //    }
  }
}

