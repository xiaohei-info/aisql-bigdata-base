package org.aisql.bigdata.base.util

import scalaj.http.{Http, HttpOptions}

/**
  * Author: xiaohei
  * Date: 2019/9/18
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
object NetUtil {
  def postRequest(host: String, params: String) = {
    Http(host).postData(params)
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .option(HttpOptions.readTimeout(10000)).asString
  }
}
