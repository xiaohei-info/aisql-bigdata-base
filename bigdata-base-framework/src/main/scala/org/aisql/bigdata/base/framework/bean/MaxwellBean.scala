package org.aisql.bigdata.base.framework.bean

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.beans.BeanProperty

/**
  * Author: xiaohei
  * Date: 2019/10/22
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */

class MaxwellBean extends Serializable {
  /**
    * 数据库名
    **/
  @BeanProperty
  var database: String = _

  /**
    * 表名
    **/
  @BeanProperty
  var table: String = _

  /**
    * 操作类型,insert、update or delete
    **/
  @BeanProperty
  var `type`: String = _

  /**
    * 数据集,json字符串
    **/
  @BeanProperty
  var data: String = _

  /**
    * 操作的时间戳
    **/
  @BeanProperty
  var ts: java.sql.Timestamp = _

  /**
    *
    **/
  @BeanProperty
  var xid: Long = _

  /**
    *
    **/
  @BeanProperty
  var commit: Boolean = _

  /**
    * 如果为update,旧数据保存在old中,json字符串
    **/
  @BeanProperty
  var old: String = _

  override def toString = {
    s"$database,$table," + this.`type` + s",$data,$ts,$xid,$commit,$old"
  }

  def toJSONString = {
    val json = new JSONObject()
    json.put("database", database)
    json.put("table", table)
    json.put("type", `type`)
    json.put("data", JSON.parseObject(data))
    json.put("ts", ts)
    json.put("xid", xid)
    json.put("commit", commit)
    json.put("old", JSON.parseObject(old))
    json.toJSONString
  }
}
