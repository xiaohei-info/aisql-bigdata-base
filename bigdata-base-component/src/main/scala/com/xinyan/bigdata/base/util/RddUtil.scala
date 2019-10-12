package com.xinyan.bigdata.base.util

import com.alibaba.fastjson.JSONObject
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField}

/**
  * Author: xiaohei
  * Date: 2019/9/18
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
object RddUtil {
  /**
    * 随机划分rdd数据块
    *
    * @param totalCount rdd总数量
    * @param step       步长,每个rdd块的数据量,单位为百万
    * @return rdd随机划分数据块的权重数组
    **/
  def rddSplitWeights(totalCount: Long, step: Double): Array[Double] = {
    val stepCount = (totalCount / (step * 10000)).toInt
    if (stepCount > 1) {
      val weight = 10.0 / stepCount

      var weightsStr = ""
      for (i <- 0 until stepCount - 1) weightsStr += weight + ","
      weightsStr = weightsStr.substring(0, weightsStr.length - 1)

      val weights = weightsStr.split(",").map(_.toDouble)
      val lastWeight = 10.0 - weights.sum
      weights ++ Array[Double](lastWeight)
    } else Array[Double](0)
  }

  /**
    *
    * @param obj 对象实例
    * @return
    */
  def getStructs(obj: Object): Array[StructField] = {
    val clazz = obj.getClass
    val fields = clazz.getDeclaredFields
    fields.map {
      x =>
        x.setAccessible(true)
        val fieldName = x.getName
        x.setAccessible(false)
        StructField(fieldName, StringType, nullable = true)
    }
  }

  /**
    * spark row 转 JSONObject
    * @param row
    * @return
    */
  def row2Object(fieldNames: Array[String], row: Row): JSONObject = {
    val result = new JSONObject()
    fieldNames.foreach {
      fieldName =>
        result.put(fieldName, row.getAs[String](fieldName))
    }
    result
  }

}
