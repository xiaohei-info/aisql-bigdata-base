package org.aisql.bigdata.base.framework.hive

import org.aisql.bigdata.base.framework.Serviceable
import org.aisql.bigdata.base.framework.enums.TableType
import org.slf4j.LoggerFactory


/**
  * Author: xiaohei
  * Date: 2019/9/9
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
trait BaseHiveService[E, R] extends Serviceable with Serializable {

  protected val dao: BaseHiveDao[E, R]

  /**
    * 全表查询
    **/
  def selectAll()(implicit env: E): R = {
    select(cols = Seq.empty, whereStr = "", limitNum = -1)
  }

  /**
    * 根据列名检索全表
    *
    * @param cols 列名
    **/
  def selectAllWithCols(cols: Seq[String])
                       (implicit env: E): R = {
    select(cols, whereStr = "", limitNum = -1)
  }

  /**
    * 根据where条件检索全表
    *
    * @param whereStr where条件
    **/
  def selectAllByWhere(whereStr: String)
                      (implicit env: E): R = {
    select(cols = Seq.empty, whereStr, limitNum = -1)
  }

  /**
    * 根据列表与where条件检索全表
    *
    * @param cols     列名
    * @param whereStr where条件字符串
    **/
  def selectAllWithColsByWhere(cols: Seq[String], whereStr: String)
                              (implicit env: E): R = {
    select(cols, whereStr, limitNum = -1)
  }


  /**
    * demo数据检索
    *
    * @return 该表的1000条样本
    **/
  def selectDemo()(implicit env: E): R = {
    select(cols = Seq.empty, whereStr = "", limitNum = 1000)
  }

  /**
    * 根据列名检索demo数据
    *
    * @param cols 列名
    * @return 该表的1000条样本
    **/
  def selectDemoWithCols(cols: Seq[String])
                        (implicit env: E): R = {
    select(cols, whereStr = "", limitNum = 1000)
  }

  /**
    * 根据where条件检索demo数据
    *
    * @param whereStr where条件
    * @return 该表的1000条样本
    **/
  def selectDemoByWhere(whereStr: String)
                       (implicit env: E): R = {
    select(cols = Seq.empty, whereStr, limitNum = 1000)
  }

  /**
    * 根据列表与where条件检索demo数据
    *
    * @param cols     列名
    * @param whereStr where条件字符串
    * @return 该表的1000条样本
    **/
  def selectDemoWithColsByWhere(cols: Seq[String], whereStr: String)
                               (implicit env: E): R = {
    select(cols, whereStr, limitNum = 1000)
  }

  /**
    * 从hive表中读取数据
    *
    * @param cols     读取的列
    * @param whereStr where条件语句,如 col1>10 and col2<=20
    * @param limitNum 限制条数
    * @return 不同引擎的读取结果,如spark的rdd
    **/
  def select(cols: Seq[String], whereStr: String, limitNum: Int)
            (implicit env: E): R = {
    println(s"data type: ${dao.TABLE_TYPE}")
    dao.TABLE_TYPE match {
      case TableType.TABLE => dao.fromHive(cols, whereStr, limitNum)
      case TableType.PARQUET => dao.fromParquet(cols, whereStr, limitNum)
      case _ => dao.fromHive(cols, whereStr, limitNum)
    }
  }

  /**
    * 从hdfs中读取数据
    *
    * @param sperator 文本分隔符
    * @return 不同引擎的读取结果,如spark的rdd
    **/
  def fromTextFile(sperator: String)(implicit env: E): R = {
    dao.fromTextFile(sperator)
  }

  /**
    * 将不同引擎的计算结果写为hive分区表,如spark的rdd
    *
    * @param partitionKeys 分区字段列表,如果为空则不分区
    * @param result        数据结果
    **/
  def createPartitionTable(partitionKeys: Seq[String], result: R)
                          (implicit env: E): Unit = {
    dao.saveAsTable(partitionKeys, result)
  }

  /**
    * 将不同引擎的计算结果写为hive表,如spark的rdd
    *
    * @param result 数据结果
    **/
  def createTable(result: R)(implicit env: E): Unit = {
    createPartitionTable(partitionKeys = Seq.empty, result)
  }

  /**
    * 将不同引擎的计算结果写入hive表中,如spark的rdd
    * 将会覆盖原有值(Overwrite)
    *
    **/
  def insertIntoOverwrite(result: R)(implicit env: E): Unit = {
    dao.insertInto(isOverwrite = true, result)
  }

  /**
    * 将不同引擎的计算结果写入hive表中,如spark的rdd
    * 不会覆盖原有值(Append)
    *
    **/
  def insertInto(result: R)(implicit env: E): Unit = {
    dao.insertInto(isOverwrite = false, result)
  }

  /**
    * 写入hdfs文本文件
    **/
  def saveAsTextFile(result: R)(implicit env: E): Unit = {
    dao.saveAsTextFile(result)
  }

}
