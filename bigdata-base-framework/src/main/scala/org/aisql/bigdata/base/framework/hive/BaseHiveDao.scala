package org.aisql.bigdata.base.framework.hive

import org.aisql.bigdata.base.framework.Daoable
import org.aisql.bigdata.base.framework.enums.TableType
import org.aisql.bigdata.base.framework.enums.TableType.TableTypeEnum

/**
  * Author: xiaohei
  * Date: 2019/8/29
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
trait BaseHiveDao[E, R] extends Daoable with Serializable {

  /**
    * 数据库名
    **/
  val DATABASE: String

  /**
    * 表名
    **/
  val TABLE: String

  /**
    * 全表名
    **/
  val FULL_TABLENAME: String

  /**
    * HDFS路径
    **/
  val HDFS_PATH: String

  /**
    * 表类型,table or parquet,默认为table,可在子类中重写
    **/
  val TABLE_TYPE: TableTypeEnum = TableType.TABLE

  /**
    * 从hive表中读取数据
    *
    * @param cols     读取的列
    * @param whereStr where条件语句,如 col1>10 and col2<=20
    * @param limitNum 限制条数
    * @return 不同引擎的读取结果,如spark的rdd
    **/
  def fromHive(cols: Seq[String], whereStr: String, limitNum: Int)
              (implicit env: E): R

  /**
    * 从parquet文件中读取数据
    *
    * @param cols     读取的列
    * @param whereStr where条件语句,如 col1>10 and col2<=20
    * @param limitNum 限制条数
    * @return 不同引擎的读取结果,如spark的rdd
    **/
  def fromParquet(cols: Seq[String], whereStr: String, limitNum: Int)
                 (implicit env: E): R

  /**
    * 从hdfs读取文本文件
    *
    * @param sperator 文本分隔符
    * @return 不同引擎的读取结果,如spark的rdd
    **/
  def fromText(sperator: String)(implicit env: E): R

  /**
    * 将不同引擎的计算结果写为hive表,如spark的rdd
    *
    * @param partitionKeys 分区字段列表,如果为空则不分区
    **/
  def saveAsTable(partitionKeys: Seq[String] = Seq.empty, result: R)
                 (implicit env: E): Unit

  /**
    * 将不同引擎的计算结果写入hive表中,如spark的rdd
    *
    * @param isOverwrite 是否覆盖写入,true为overwrite,false为append
    **/
  def insertInto(isOverwrite: Boolean, result: R)
                (implicit env: E): Unit

  /**
    * 将不同引擎的计算结果写为parquet文件,如spark的rdd
    *
    * @param partitionKeys 分区字段列表,如果为空则不分区
    **/
  def writeParquet(partitionKeys: Seq[String] = Seq.empty, result: R)
                  (implicit env: E): Unit

  /**
    * 将不同引擎的计算结果写入hdfs路径中,如spark的rdd
    * */
  def saveAsTextFile(result: R)(implicit env: E): Unit


}
