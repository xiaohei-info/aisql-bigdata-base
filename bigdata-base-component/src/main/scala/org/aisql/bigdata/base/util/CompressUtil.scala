package org.aisql.bigdata.base.util

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util.zip.GZIPInputStream

import com.alibaba.fastjson.{JSON, JSONObject}
import org.aisql.bigdata.base.java.CryptTool
import org.apache.commons.lang.StringUtils

/**
  * Author: xiaohei
  * Date: 2019/9/18
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
object CompressUtil {
  def decompressData(str: String): JSONObject = {
    if(StringUtils.isBlank(str)) {
      return null
    }
    var result: JSONObject = null
    var bis: ByteArrayInputStream = null
    var gzin: GZIPInputStream = null
    var ois: ObjectInputStream = null
    try {
      bis = new ByteArrayInputStream(CryptTool.hexString2ByteArray(str.replace("\"", "")))
      //建立gzip解压输入流
      gzin = new GZIPInputStream(bis)
      //建立对象序列化输入流
      ois = new ObjectInputStream(gzin)
      //按制定类型还原对象
      result = JSON.parseObject(ois.readObject.asInstanceOf[String])
    } catch {
      case e: Exception =>
        println("CommonUtil | decompressData | Exception {}", e)
    } finally {
      if(ois != null) {
        ois.close()
      }
    }
    result
  }
}
