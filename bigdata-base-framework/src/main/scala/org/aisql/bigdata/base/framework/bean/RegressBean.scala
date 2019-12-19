package org.aisql.bigdata.base.framework.bean

/**
  * Author: xiaohei
  * Date: 2019/12/19
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */

/**
  * @param sampleTable         样本表名
  * @param sampleIdCardField   样本表身份证字段名称
  * @param sampleCardNameField 样本表姓名字段名称
  * @param loanDayField        回溯时间字段名称
  * @param maxCount            最大允许的回溯数量大小,与当前driver内存有关
  * @param idCardField         数据表身份证字段名
  * @param cardNameField       数据表姓名字段名
  **/
case class RegressBean(sampleTable: String, sampleIdCardField: String, sampleCardNameField: String, loanDayField: String
                       , maxCount: Long, idCardField: String, cardNameField: String)
