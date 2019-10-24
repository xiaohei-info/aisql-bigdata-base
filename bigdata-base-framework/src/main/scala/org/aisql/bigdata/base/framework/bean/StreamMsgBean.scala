package org.aisql.bigdata.base.framework.bean

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * Author: xiaohei
  * Date: 2019/10/24
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
class StreamMsgBean extends Serializable {

  /**
    * 业务类型标识
    * ${database}-${table} ，与最终Hive结构表对应
    **/
  var btype: String = _

  /**
    * 数据记录唯一标识
    * 32位uuid，可以为唯一的业务主键，作为kafka消息的key
    **/
  var recordgid: String = _

  /**
    * 数据获取的业务时间
    * 13位时间戳，create_time
    **/
  var gtime: String = _

  /**
    * 数据推送时间
    * 13位时间戳，当前处理时间
    **/
  var utime: String = _

  /**
    * 业务数据json串
    * 业务数据
    **/
  var data: String = _

  override def toString = {
    s"$btype,$recordgid,$gtime,$utime,$data"
  }

  def toJSONString = {
    val json = new JSONObject()
    json.put("btype", btype)
    json.put("recordgid", recordgid)
    json.put("gtime", gtime)
    json.put("utime", utime)
    json.put("data", JSON.parseObject(data))
    json.toJSONString
  }
}
