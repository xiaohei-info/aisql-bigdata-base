package com.xinyan.bigdata.base.connector.realtime.sinks

import com.alibaba.fastjson.JSONObject
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}

import scala.collection.JavaConversions._
import scala.util.Try
import scala.util.control.Breaks

/**
  * Author: xiaohei
  * Date: 2018/4/12
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
class HBaseSink(zhHost: String, confFile: String) extends Serializable {
  lazy val connection = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zhHost)
    hbaseConf.addResource(confFile)
    val conn = ConnectionFactory.createConnection(hbaseConf)
    sys.addShutdownHook {
      conn.close()
    }
    conn
  }

  //todo:封装参考dbconnector
  def getTable(tableName: TableName): Table = {
    connection.getTable(tableName)
  }

  def close() = {
    connection.close()
  }

  /**
    * 判断指定 rowkey 的数据是否存在
    *
    * @param htable hbase 表
    * @param rowkey 主键
    * @return 是否存在
    */
  def isExist(htable: String, rowkey: String): Boolean = {
    connection.getTable(TableName.valueOf(htable)).exists(new Get(Bytes.toBytes(rowkey)))
  }

  def get(rowkey: String, htable: String, colFamily: String, cols: Seq[String]): Option[JSONObject] = {
    val table = connection.getTable(TableName.valueOf(htable))
    val get = new Get(Bytes.toBytes(rowkey))
    cols.foreach(c => get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(c)))

    val resultData = if (table.exists(get)) {
      val getResult = table.get(get)
      val rsMap = getResult.getFamilyMap(colFamily.getBytes).map(x => (Bytes.toString(x._1), Bytes.toString(x._2)))
      val jsonObject = new JSONObject()
      rsMap.foreach(kv => jsonObject.put(kv._1, kv._2))
      Some(jsonObject)
    } else {
      None
    }
    table.close()
    resultData
  }

  @deprecated("批量get没有判断是否存在,有NullPointerException风险")
  def get(rowkeys: Seq[String], htable: String, colFamily: String, cols: Seq[String]): Iterable[JSONObject] = {
    val table = connection.getTable(TableName.valueOf(htable))
    val gets = rowkeys.map {
      rowkey =>
        val get = new Get(Bytes.toBytes(rowkey))
        cols.foreach(c => get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(c)))
        get
    }
    val getResult = table.get(gets)
    table.close()
    getResult.map {
      y =>
        val json = new JSONObject()
        val rsMap = y.getFamilyMap(colFamily.getBytes).map(x => (Bytes.toString(x._1), Bytes.toString(x._2)))
        rsMap.foreach(kv => json.put(kv._1, kv._2))
        json
    }
  }

  /**
    * 获取指定列族的 Result 信息
    *
    * @param result    Result
    * @param colFamily 列族名
    * @return 形如 Map(colName, colValue) 的 JSONObject
    */
  private def getResult(result: Result, colFamily: String): JSONObject = {
    var resultObj: JSONObject = null
    val resultMap = result.getFamilyMap(colFamily.getBytes).map(x => (Bytes.toString(x._1), Bytes.toString(x._2)))
    if (resultMap == null) {
      return resultObj
    }
    resultObj = new JSONObject()
    resultMap.foreach {
      case (k, v) =>
        resultObj.put(k, v)
    }
    resultObj
  }

  /**
    * 批量查询 HBase 数据
    *
    * @param tableName HBase 表名
    * @param rowKeys   rowKey Set
    * @param cols      需要保留的列，默认查询所有列
    * @param colFamily
    * @return 结果集 类似于 Map(rowKey, (colName, colValue)) JSONObject
    */
  @throws(classOf[Exception])
  def getByRowKeys(tableName: String, rowKeys: Set[String], cols: Set[String] = Set.empty[String], colFamily: String = "info"): JSONObject = {
    val resultObj: JSONObject = new JSONObject()
    var table: Table = null
    try {
      table = connection.getTable(TableName.valueOf(tableName))
      val gets = rowKeys.map {
        rowKey =>
          val get = new Get(Bytes.toBytes(rowKey))
          cols.foreach(c => get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(c)))
          get
      }
      val loop = new Breaks
      val results: Array[Result] = table.get(gets.toList)
      results.foreach {
        result =>
          loop.breakable {
            //skip blank result to avoid getting NullPointerException
            if (result == null || result.getRow == null) {
              loop.break
            }
            resultObj.put(Bytes.toString(result.getRow), getResult(result, colFamily))
          }
      }
    } finally {
      if (table != null) {
        table.close()
      }
    }
    resultObj
  }

  def scan(rowkeyPrefix: String, htable: String, colFamily: String, cols: Seq[String], startSuffix: String
           , endSuffix: String): Iterable[JSONObject] = {
    val table = connection.getTable(TableName.valueOf(htable))
    val scan = new Scan()
    scan.setStartRow(Bytes.toBytes(rowkeyPrefix + startSuffix))
    scan.setStopRow(Bytes.toBytes(rowkeyPrefix + endSuffix))
    cols.foreach(c => scan.addColumn(Bytes.toBytes(c), Bytes.toBytes(colFamily)))
    val resultScanner = table.getScanner(scan)
    table.close()
    resultScanner.map {
      y =>
        val json = new JSONObject()
        val rsMap = y.getFamilyMap(colFamily.getBytes).map(x => (Bytes.toString(x._1), Bytes.toString(x._2)))
        rsMap.foreach(kv => json.put(kv._1, kv._2))
        json
    }
  }

  /**
    * 通过 rowKey 扫描符合条件的数据
    *
    * @param rowKeyPrefix rowkey 前缀
    * @param tableName    hbase 表名
    * @param colFamily    列族
    * @param cols         需要保留的列
    * @param startSuffix  开始前缀
    * @param endSuffix    结束前缀
    * @return
    */
  @throws(classOf[Exception])
  def scanByRowKey(rowKeyPrefix: String, tableName: String, colFamily: String, cols: Seq[String], startSuffix: String
                   , endSuffix: String): JSONObject = {
    val resultObj: JSONObject = new JSONObject()
    var table: Table = null
    try {
      table = connection.getTable(TableName.valueOf(tableName))
      val scan = new Scan()
      scan.setStartRow(Bytes.toBytes(rowKeyPrefix + startSuffix))
      scan.setStopRow(Bytes.toBytes(rowKeyPrefix + endSuffix))
      cols.foreach(c => scan.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(c)))
      val resultScanner = table.getScanner(scan)
      val loop = new Breaks
      resultScanner.foreach {
        result =>
          loop.breakable {
            //skip blank result to avoid getting NullPointerException
            if (result == null || result.getRow == null) {
              loop.break
            }
            resultObj.put(Bytes.toString(result.getRow), getResult(result, colFamily))
          }
      }
    } finally {
      if (table != null) {
        table.close()
      }
    }
    resultObj
  }

  def put(rowkey: String, htable: String, colFamily: String, value: JSONObject): Unit = {
    val put = new Put(Bytes.toBytes(rowkey))
    value.entrySet().toList.foreach {
      entry =>
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(entry.getKey), Bytes.toBytes(String.valueOf(entry.getValue)))
    }
    val params = new BufferedMutatorParams(TableName.valueOf(htable)) //.writeBufferSize(6 * 1024 * 1024)
    val mutator = connection.getBufferedMutator(params)
    Try {
      mutator.mutate(put)
      mutator.flush()
    }.getOrElse(mutator.close())
  }

  def put(rowkey: String, htable: String, value: JSONObject, isRemoveEmpty: Boolean = false, colFamily: String = "info"): Unit = {
    val put = new Put(Bytes.toBytes(rowkey))
    value.entrySet().toList.foreach {
      entry =>
        if (isRemoveEmpty) {
          if (entry.getValue != null && entry.getValue != "") {
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(entry.getKey), Bytes.toBytes(String.valueOf(entry.getValue)))
          }
        } else {
          put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(entry.getKey), Bytes.toBytes(String.valueOf(entry.getValue)))
        }
    }
    val params = new BufferedMutatorParams(TableName.valueOf(htable)) //.writeBufferSize(6 * 1024 * 1024)
    val mutator = connection.getBufferedMutator(params)
    Try {
      mutator.mutate(put)
      mutator.flush()
    }.getOrElse(mutator.close())
  }

  def put(htable: String, colFamily: String, values: Seq[JSONObject]): Unit = {
    val puts = values.map {
      data =>
        //rowkey
        val rowKey = data.getString("rowkey")
        val put = new Put(Bytes.toBytes(rowKey))
        data.entrySet().toList.foreach {
          entry =>
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(entry.getKey), Bytes.toBytes(String.valueOf(entry.getValue)))
        }
        put
    }
    val params = new BufferedMutatorParams(TableName.valueOf(htable)) //.writeBufferSize(6 * 1024 * 1024)
    val mutator = connection.getBufferedMutator(params)
    Try {
      mutator.mutate(puts)
      mutator.flush()
    }.getOrElse(mutator.close())
  }

  def putCento(htable: String, colFamily: String, values: Seq[JSONObject]): Unit = {
    val puts = values.map {
      data =>
        //rowkey
        val rowKey = data.getString("rowkey")
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes("cento"), Bytes.toBytes(data.toJSONString))
        put
    }
    val params = new BufferedMutatorParams(TableName.valueOf(htable))
    val mutator = connection.getBufferedMutator(params)
    Try {
      mutator.mutate(puts)
      mutator.flush()
    }.getOrElse(mutator.close())
  }

  def delete(rowkey: String, htable: String): Unit = {
    val table = connection.getTable(TableName.valueOf(htable))
    val delete = new Delete(Bytes.toBytes(rowkey))
    table.delete(delete)
    table.close()
  }

  /**
    * save table
    *
    * @param tableName
    * @param put
    * @throws java.lang.Exception
    */
  @throws(classOf[Exception])
  def save(tableName: String, put: Put): Unit = {
    val table = connection.getTable(TableName.valueOf(tableName))
    table.put(put)
  }

}


object HBaseSink {
  def apply(zhHost: String, confFile: String): HBaseSink = {
    new HBaseSink(zhHost: String, confFile: String)
  }
}