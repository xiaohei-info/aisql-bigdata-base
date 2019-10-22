package org.aisql.bigdata.base.framework.bean

import com.alibaba.fastjson.JSONObject
import scala.beans.BeanProperty

/**
  * Author: xiaohei
  * Date: 2019/10/22
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */

class MaxwellBean extends Serializable {
  /**
    * 自增主键
    **/
  @BeanProperty
  var database: String = _

  /**
    * 业务流水号
    **/
  @BeanProperty
  var table: String = _

  /**
    * 宝付交易号
    **/
  @BeanProperty
  var `type`: String = _

  /**
    * 自增主键
    **/
  @BeanProperty
  var data: String = _

  /**
    * 业务流水号
    **/
  @BeanProperty
  var ts: String = _

  /**
    * 宝付交易号
    **/
  @BeanProperty
  var xid: String = _

  /**
    * 宝付交易号
    **/
  @BeanProperty
  var commit: String = _

  override def toString = {
    s"$database,$table," + this.`type` + s",$data,$ts,$xid,$commit"
  }

  def toJSONString = {
    val json = new JSONObject()
    json.put("database", database)
    json.put("table", table)
    json.put("type", `type`)
    json.put("data", data)
    json.put("ts", ts)
    json.put("xid", xid)
    json.put("commit", commit)
    json.toJSONString
  }
}
