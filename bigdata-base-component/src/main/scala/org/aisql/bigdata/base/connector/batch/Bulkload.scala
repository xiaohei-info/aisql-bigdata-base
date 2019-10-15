package org.aisql.bigdata.base.connector.batch

import com.alibaba.fastjson.JSONObject
import org.aisql.bigdata.base.util.{RddUtil, StringUtil}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Author: xiaohei
  * Date: 2018/3/15
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.infoBulkload
  */
class Bulkload(@transient spark: SparkSession) {


  /**
    * 注意分区数,一个region最多加载2048个数据块
    * 列族默认为info
    * 强制添加date字段
    *
    * @param hbaseHost       hbase-zk地址
    * @param hbaseTableName  导入的hbase表名,包含namespace
    * @param tableDf         spark.table or spark.sql 读取生成的hive数据源
    * @param fileSavePath    bulkload数据保存路径,默认为/user/xy_app_spark/bulkload/${db-table},规定传入db.table
    * @param defRowkeyRule   rowkey生成函数,如:row.getAs[String]("rowkey")
    * @param load2HBase      是否在程序中直接load到hbase,数据量大的建议使用hbase shell工具,默认为false
    * @param removeColumnOpt 移除列：用于把rowkey的那一列移除
    *
    *
    *                        修改：20181220
    *                        bulkload 里面不能过滤空字段，会影响后续程序（比如：实时流gateway），如果字段值是空的话，字段还是要先保留着（如果要过滤掉空值的字段，可以之后定好规则，保证后续不被影响也是可取的）
    **/
  def bulkload2HBase(hbaseHost: String, hbaseTableName: String, tableDf: DataFrame,
                     fileSavePath: String, defRowkeyRule: Row => String,
                     load2HBase: Boolean = false, updateDate: String, loadPath: Option[String] = None, isRemoveEmpty: Boolean = false,
                     removeColumnOpt: Option[String] = None) = {
    val conf = HBaseConfiguration.create()
    //    conf.addResource(new Path("/etc/hbase/hbase-conf/hbase-site.xml"))
    conf.set(HConstants.ZOOKEEPER_QUORUM, hbaseHost)
    conf.setInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 2048)
    conf.set("dfs.replication", "2")
    //设置Hfile压缩
    conf.set("hfile.compression", "snappy")
    conf.set("hbase.defaults.for.version.skip", "true")
    conf.set("hbase.regionserver.codecs", "snappy")
    conf.set("hbase.mapreduce.hfileoutputformat.table.name", hbaseTableName)
    conf.set(HConstants.TEMPORARY_FS_DIRECTORY_KEY, "/user/xy_app_spark/hbase-staging")
    println(s"$hbaseTableName set config done.")

    //create a HTable
    //使用新版Table API
    val connection: Connection = ConnectionFactory.createConnection(conf)
    val table: Table = connection.getTable(TableName.valueOf(hbaseTableName))

    println(s"hbase getTable $hbaseTableName done.")

    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoad(job, table.getDescriptor, connection.getRegionLocator(TableName.valueOf(hbaseTableName)))
    //    HFileOutputFormat2.configureIncrementalLoadMap(job, table.getDescriptor)

    println(s"$hbaseTableName job HFileOutputFormat2 configureIncrementalLoad done.")

    val originColumns = tableDf.columns
    val columnsBuf = new ArrayBuffer[String]()
    columnsBuf ++= originColumns
    //如果有移除列，移除
    if (removeColumnOpt.nonEmpty) {
      val removeColumn = removeColumnOpt.get
      for (x <- originColumns.indices) {
        if (originColumns(x).equals(removeColumn)) {
          columnsBuf.remove(x)
        }
      }
    }

    val columns = columnsBuf.toArray

    //    val df = new DecimalFormat("#.0000")
    val beforeHFile = tableDf.rdd.map {
      row =>
        //表中已经包含rowkey字段
        val value = if (columns.contains("date")) columns.zip(row.mkString("####").split("####", -1))
        else (Array[String]("date") ++ columns).zip(Array[String](updateDate) ++ row.mkString("####").split("####", -1))

        val key = defRowkeyRule(row)
        if (isRemoveEmpty) {
          // 去除插入表中的空数据
          (key, value.filter(entry => StringUtil.isNotNull(entry._2)))
        } else {
          (key, value)
        }
    }

    val rdd: RDD[(ImmutableBytesWritable, KeyValue)] = beforeHFile.sortByKey()
      //form the data as (ImmutableBytesWritable, KeyValue) to get ready for bulkloading afterword
      .flatMap {
      t =>
        val rk2Bytes = Bytes.toBytes(t._1)
        val immutableBytesWritable = new ImmutableBytesWritable(rk2Bytes)
        val values = t._2
        //sort the columnFamily + qualifier
        values.sortBy(_._1).map {
          value =>
            val qualifier = value._1
            val cellValue2Bytes = Bytes.toBytes(value._2)
            val kv = new KeyValue(rk2Bytes, Bytes.toBytes("info"), Bytes.toBytes(qualifier), cellValue2Bytes)
            (immutableBytesWritable, kv)
        }
    }

    println(s"$hbaseTableName rdd got.")

    /**
      * 1:Generate Hfiles
      * 2:Bulkload Hfiles to HBase（org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles）
      */
    val savePath = if (loadPath.isEmpty) {
      s"/user/xy_app_spark/bulkload/${fileSavePath.replace(".", "-")}"
    } else loadPath.get
    //路径已存在处理
    val hdfsPath = new Path(savePath)
    val hdfs = FileSystem.get(new org.apache.hadoop.conf.Configuration())
    if (hdfs.exists(hdfsPath)) hdfs.delete(hdfsPath, true)
    rdd.saveAsNewAPIHadoopFile(savePath, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], conf)

    println(s"$hbaseTableName saveAsNewAPIHadoopFile done.")

    val all = FsAction.ALL
    val fsPermission = new FsPermission(all, all, all)
    hdfs.setPermission(hdfsPath, fsPermission)
    hdfs.setVerifyChecksum(true)

    //    if (load2HBase) {
    //      //todo
    //      //Bulkload Hfiles to Hbashell
    //      //      val bulkLoader = new LoadIncrementalHFiles(conf)
    //      //      val admin = connection.getAdmin
    //      //      bulkLoader.doBulkLoad(hdfsPath, admin, table, connection.getRegionLocator(TableName.valueOf(hbaseTableName)))
    //      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //      import sys.process._
    //      println(s"${df.format(new Date())} $hbaseTableName start distcp...")
    //      s"/home/xy_app_spark/shell/tool-distcp.sh ${savePath.substring(19)}".!
    //      println(s"${df.format(new Date())} $hbaseTableName distcp done!")
    //      println(s"${df.format(new Date())} $hbaseTableName start remote bulkload...")
    //      s"ssh cdh85-55 hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dcreate.table=no -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=2048 ${savePath.substring(19)} $hbaseTableName".!
    //      println(s"${df.format(new Date())} $hbaseTableName remote bulkload done!")
    //    }
  }

  def bulkloadHFiles(hbaseHost: String, hbaseTableName: String, hdfsPath: String) = {
    val conf = HBaseConfiguration.create()
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))
    conf.set(HConstants.ZOOKEEPER_QUORUM, hbaseHost)
    conf.setInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 2048)
    conf.set("dfs.replication", "2")
    //设置Hfile压缩
    conf.set("hfile.compression", "snappy")
    conf.set("hbase.defaults.for.version.skip", "true")
    conf.set("hbase.regionserver.codecs", "snappy")
    //create a HTable
    //使用新版Table API
    val connection: Connection = ConnectionFactory.createConnection(conf)
    val table: Table = connection.getTable(TableName.valueOf(hbaseTableName))

    //Bulkload Hfiles to Hbase
    val bulkLoader = new LoadIncrementalHFiles(conf)
    val admin = connection.getAdmin
    bulkLoader.doBulkLoad(new Path(hdfsPath), admin, table, connection.getRegionLocator(TableName.valueOf(hbaseTableName)))
  }

  /**
    * bulkload 去除冗余数据，默认取最新上传的数据
    * 对每个分区进行排序去重，减少 shuffle 的量
    *
    * @param dataFrame
    * @param defRowkeyRule
    * @param distinctAttr
    */
  def removeReduances(dataFrame: DataFrame, defRowkeyRule: Row => String, distinctAttr: String): DataFrame = {
    var rowkey: String = null
    var distinctVal: String = null

    val rows: RDD[Row] = dataFrame.rdd.map {
      row =>
        (defRowkeyRule(row), row.getAs[String](distinctAttr), row)
    }.filter(x => StringUtils.isNotBlank(x._2)).mapPartitions {
      // 每个分区做排序、去重
      singlePartDataSet =>
        singlePartDataSet.toSeq.groupBy(_._1).map(_._2.maxBy(_._2)).toIterator
    }.groupBy {
      // 按主键分组
      data =>
        data._2
    }.map(
      // 分区排序后去重后的数据做全部的排序去重
      dataSet =>
        dataSet._2.toList.maxBy(_._2)._3
    )
    spark.createDataFrame(rows, dataFrame.schema)
  }

  /**
    * bulkload 去除冗余数据，默认取最新上传的数据
    * 对每个分区进行排序去重，减少 shuffle 的量
    *
    * @param dataFrame
    * @param defRowkeyRule
    * @param distinctAttr
    */
  def removeReduances(dataFrame: DataFrame, defRowkeyRule: Row => String, distinctAttr: String, defParse: JSONObject => JSONObject): DataFrame = {
    val fieldNames = dataFrame.schema.fields.map(_.name)
    val resultRdd: RDD[JSONObject] = dataFrame.rdd.map {
      row =>
        (defRowkeyRule(row), row.getAs[String](distinctAttr), row)
    }.filter(x => StringUtils.isNotBlank(x._2)).mapPartitions {
      // 每个分区做排序、去重
      singlePartDataSet =>
        singlePartDataSet.toSeq.groupBy(_._1).map(_._2.maxBy(_._2)).toIterator
    }.groupBy {
      // 按主键分组
      data =>
        data._2
    }.map(
      // 分区排序后去重后的数据做全部的排序去重
      dataSet =>
        dataSet._2.toList.maxBy(_._2)._3
    ).mapPartitions {
      data =>
        // 数据解析操作
        val result: ArrayBuffer[JSONObject] = ArrayBuffer[JSONObject]()
        while (data.hasNext) {
          try {
            result.append(defParse(RddUtil.row2Object(fieldNames, data.next())))
          } catch {
            case ex =>
              println("Bulkload | removeReduances | Exception: {}", ex)
          }
        }
        result.toIterator
    }
    spark.read.json(resultRdd.map(_.toJSONString))
  }

}

object Bulkload {
  implicit def convert2BulkLoad(spark: SparkSession) = new Bulkload(spark)
}
