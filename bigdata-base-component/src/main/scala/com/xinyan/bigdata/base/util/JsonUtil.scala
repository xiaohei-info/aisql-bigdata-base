package com.xinyan.bigdata.base.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.ArrayBuffer

/**
  * Author: xiaohei
  * Date: 2019/9/18
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
object JsonUtil {
  def parseJsonSafe(str: String): Option[JSONObject] = {
    try {
      val json = JSON.parseObject(str)
      Some(json)
    } catch {
      case e: Exception => None
    }
  }

  def mergeJSONObjects(objs: ArrayBuffer[JSONObject]): JSONObject = {
    val result = new JSONObject()
    if (objs == null || objs.isEmpty) {
      return result
    }
    objs.foreach {
      obj => {
        if (obj != null) {
          obj.keySet().toArray.foreach {
            key =>
              result.put(String.valueOf(key), obj.get(key))
          }
        }
      }
    }
    result
  }

  def JSONArray2ArrayBuffer(data: JSONArray): ArrayBuffer[JSONObject] = {
    if (data == null || data.size() <= 0) {
      return null
    }
    val result = ArrayBuffer[JSONObject]()
    var obj: JSONObject = new JSONObject()
    for (i <- 0 until data.size()) {
      obj = data.getJSONObject(i)
      result.append(obj)
    }
    result
  }
}
