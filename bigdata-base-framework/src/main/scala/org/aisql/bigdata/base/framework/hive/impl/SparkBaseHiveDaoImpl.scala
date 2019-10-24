package org.aisql.bigdata.base.framework.hive.impl

import org.aisql.bigdata.base.framework.hive.BaseHiveDao
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
  * Author: xiaohei
  * Date: 2019/9/4
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
trait SparkBaseHiveDaoImpl[B] extends BaseHiveDao[SparkSession, RDD[B]] {

  /**
    * 从hive表中读取数据
    *
    * @param cols     读取的列
    * @param whereStr where条件语句,如 col1>10 and col2<=20
    * @param limitNum 限制条数
    * @return 不同引擎的读取结果,如spark的rdd
    **/
  override def fromHive(cols: Seq[String], whereStr: String, limitNum: Int)
                       (implicit env: SparkSession): RDD[B] = {
    readData(FULL_TABLENAME, cols, whereStr, limitNum)
  }

  /**
    * 从parquet文件中读取数据
    *
    * @param cols     读取的列
    * @param whereStr where条件语句,如 col1>10 and col2<=20
    * @param limitNum 限制条数
    * @return 不同引擎的读取结果,如spark的rdd
    **/
  override def fromParquet(cols: Seq[String], whereStr: String, limitNum: Int)
                          (implicit env: SparkSession): RDD[B] = {
    val tmpTableName = FULL_TABLENAME + "_tmpview"
    println(s"create tmp view: $tmpTableName")
    env.read.parquet(HDFS_PATH).createOrReplaceTempView(tmpTableName)
    readData(tmpTableName, cols, whereStr, limitNum)
  }

  /**
    * 从hdfs读取文本文件
    *
    * @param sperator 文本分隔符
    * @return 不同引擎的读取结果,如spark的rdd
    **/
  override def fromText(sperator: String)(implicit env: SparkSession): RDD[B] = {
    val rdd = env.sparkContext.textFile(HDFS_PATH).map(_.split(sperator))
    transText2Bean(rdd)
  }

  /**
    * 将不同引擎的计算结果写为hive表,如spark的rdd
    *
    * @param partitionKeys 分区字段列表,如果为空则不分区
    **/
  override def saveAsTable(partitionKeys: Seq[String] = Seq.empty, result: RDD[B])
                          (implicit env: SparkSession): Unit = {
    val df = transRdd2Df(result)
    println(s"save to $FULL_TABLENAME")
    if (partitionKeys.nonEmpty) {
      println(s"partition keys: ${partitionKeys.mkString(",")}")
      df.write.partitionBy(partitionKeys: _*).saveAsTable(FULL_TABLENAME)
    } else {
      println("no partition keys, save as normal table")
      df.write.saveAsTable(FULL_TABLENAME)
    }
  }

  /**
    * 将不同引擎的计算结果写入hive表中,如spark的rdd
    *
    * @param isOverwrite 是否覆盖写入,true为overwrite,false为append
    **/
  override def insertInto(isOverwrite: Boolean, result: RDD[B])
                         (implicit env: SparkSession): Unit = {
    val df = transRdd2Df(result)
    println(s"insert into $FULL_TABLENAME, isOverwrite: $isOverwrite")
    if (isOverwrite) {
      df.write.mode(SaveMode.Overwrite).insertInto(FULL_TABLENAME)
    } else {
      df.write.mode(SaveMode.Append).insertInto(FULL_TABLENAME)
    }
  }

  /**
    * 将不同引擎的计算结果写为parquet文件,如spark的rdd
    *
    * @param partitionKeys 分区字段列表,如果为空则不分区
    **/
  override def writeParquet(partitionKeys: Seq[String] = Seq.empty, result: RDD[B])
                           (implicit env: SparkSession): Unit = {
    val df = transRdd2Df(result)
    println(s"write parquet to $HDFS_PATH")
    if (partitionKeys.nonEmpty) {
      println(s"partition keys: ${partitionKeys.mkString(",")}")
      df.write.partitionBy(partitionKeys: _*).parquet(HDFS_PATH)
    } else {
      println("no partition keys, save as normal table")
      df.write.parquet(HDFS_PATH)
    }
  }

  /**
    * 将不同引擎的计算结果写入hdfs路径中,如spark的rdd
    **/
  override def saveAsTextFile(result: RDD[B])(implicit env: SparkSession): Unit = {
    result.map(_.toString).saveAsTextFile(HDFS_PATH)
  }

  /**
    * 读取hive数据时,将DadaFrame的Row转化为具体的bean对象
    *
    * @param df Row对象
    * @return 具体的bean对象
    **/
  protected def transDf2Rdd(df: DataFrame)
                           (implicit env: SparkSession): RDD[B]

  /**
    * 写入hive表时,将RDD转换为DataFrame
    *
    * @param rdd rdd对象
    * @return DataFrame对象
    **/
  protected def transRdd2Df(rdd: RDD[B])
                           (implicit env: SparkSession): DataFrame

  /**
    * 读取hdfs text文件时,将文本数据(数组)转化为具体的bean对象
    *
    * @param arrRdd 使用分隔符split之后的数据数组
    * @return 具体的bean对象
    **/
  protected def transText2Bean(arrRdd: RDD[Array[String]]): RDD[B]


  /**
    * 读取指定数据表(hive or tmpview)
    **/
  private def readData(tableName: String, cols: Seq[String], whereStr: String, limitNum: Int)
                      (implicit env: SparkSession): RDD[B] = {
    val colCondition = if (cols.nonEmpty) cols.mkString(",") else "*"
    var sql = s"select $colCondition from $tableName"
    sql = if (whereStr.nonEmpty) s"$sql where $whereStr" else sql
    sql = if (limitNum > 0) s"$sql limit $limitNum"
    else sql
    println(s"exec sql: $sql")
    transDf2Rdd(env.sql(sql))
  }
}
