package com.xinyan.bigdata.base.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * Author: xiaohei
  * Date: 2017/5/14
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
object DateUtil {
  /**
    * 判断是否是周末
    *
    * @param date
    * @return
    */
  def isWeekOrWork(date: String): String = {
    val myformat = new SimpleDateFormat("yyyy-MM-dd")
    var dnow = new Date()
    if (date != "" && date != null) {
      dnow = myformat.parse(date)
    }
    val cal = Calendar.getInstance()
    cal.setTime(dnow)
    val week = cal.get(Calendar.DAY_OF_WEEK)
    if (week == 1 || week == 7) "week" else "work"
  }

  /**
    * 计算两个日期之间的天数差,日期格式为yyyy-MM-dd
    *
    * @param first  减数
    * @param second 被减数
    * @return 天数差
    **/
  def daysDiff(first: String, second: String): Int = {
    if (first == "" || second == "") {
      return -1
    }

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(sdf.parse(first))
    val time1 = cal.getTimeInMillis
    cal.setTime(sdf.parse(second))
    val time2 = cal.getTimeInMillis
    val between_days = (time1 - time2) / (1000 * 3600 * 24)

    Integer.parseInt(String.valueOf(between_days))
  }

  /**
    * 获取指定日期往后n个月的日期,日期格式为yyyy-MM-dd
    *
    * @param date 基数日期
    * @param n    往后几个月,负数则为往前几个月
    * @return 对应日期,格式为yyyy-MM-dd
    *
    **/
  def currNMonth(date: String, n: Int): String = {
    //过去一月
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(sdf.parse(date))
    cal.add(Calendar.MONTH, n)
    val m = cal.getTime
    sdf.format(m)
  }

  /**
    * 获取指定日期往后n天的日期,日期格式为yyyy-MM-dd
    *
    * @param date 基数日期
    * @param n    往后几天,负数则为往前几天
    * @return 对应日期,格式为yyyy-MM-dd
    *
    **/
  def currNDay(date: String, n: Int): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    //取时间
    cal.setTime(sdf.parse(date))
    cal.add(Calendar.DATE, n) //把日期往后增加一天.整数往后推,负数往前移动
    val d = cal.getTime //这个时间就是日期往后推一天的结果
    sdf.format(d)
  }


  /**
    * 获取当前日期,格式为yyyy-MM-dd
    *
    * @return 当前日期
    **/
  def currDate = {
    val d = new Date()
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    sdf.format(d)
  }

  /**
    * 获取当前日期,格式为yyyy-MM-dd
    *
    * @return 当前日期
    **/
  def currTime: String = {
    val d = new Date()
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.format(d)
  }

  /**
    * 获取当前日期,格式为yyyyMMdd
    *
    * @return 当前日期
    **/
  def currDateMMdd = {
    val d = new Date()
    val sdf = new SimpleDateFormat("yyyyMMdd")
    sdf.format(d)
  }


  /**
    * 获取昨天的日期,格式为yyyy-MM-dd
    *
    * @return
    */
  def yestoday: String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(cal.getTime)
    yesterday
  }

  /**
    * 获取昨天的日期,格式为yyyyMMdd
    *
    * @return
    */
  def yestodayMMdd: String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(cal.getTime)
    yesterday
  }

  /**
    * 获取本周开始日期
    *
    * @return
    */
  def weekStartDay: String = {
    var period: String = ""
    val cal: Calendar = Calendar.getInstance()
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    //获取本周一的日期
    period = df.format(cal.getTime)
    period
  }

  /**
    * 获取本周末的时间
    *
    * @return
    */
  def weekEndDay: String = {
    var period: String = ""
    val cal: Calendar = Calendar.getInstance()
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY); //这种输出的是上个星期周日的日期，因为老外把周日当成第一天
    cal.add(Calendar.WEEK_OF_YEAR, 1) // 增加一个星期，才是我们中国人的本周日的日期
    period = df.format(cal.getTime)
    period
  }

  /**
    * 本月的第一天
    *
    * @return
    */
  def monthStartDay: String = {
    var period: String = ""
    val cal: Calendar = Calendar.getInstance()
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    cal.set(Calendar.DATE, 1)
    period = df.format(cal.getTime) //本月第一天
    period
  }

  /**
    * 本月的最后一天
    *
    * @return
    */
  def monthEndDay: String = {
    var period: String = ""
    val cal: Calendar = Calendar.getInstance()
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    cal.set(Calendar.DATE, 1)
    cal.roll(Calendar.DATE, -1)
    period = df.format(cal.getTime) //本月最后一天
    period
  }


  /**
    * 将时间戳转化成日期
    *
    * @param time   时间戳字符串
    * @param format 格式化格式
    * @return 指定格式的日期字符串
    */
  def ts2date(time: String, format: String = "yyyy-MM-dd HH:mm:ss"): String = {
    val sdf: SimpleDateFormat = new SimpleDateFormat(format)
    sdf.format(new Date(time.toLong))
  }


  /**
    *
    * 将日期转换为时间戳
    *
    * @param date   日期字符串
    * @param format 格式化格式
    * @return 13位时间戳字符串
    **/
  def date2ts(date: String, format: String = "yyyy-MM-dd HH:mm:ss"): String = {
    val sdf: SimpleDateFormat = new SimpleDateFormat(format)
    sdf.parse(date).getTime.toString
  }

  def main(args: Array[String]) {
    println(ts2date("1568787947220", "yyyy-MM-dd"))
    println(date2ts("2019-09-18 14:25:47", "yyyy-MM-dd"))
  }

}
