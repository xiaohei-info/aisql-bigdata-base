package org.aisql.bigdata.base.gojira.test.dal.dao.sparkimpl

import java.sql.Timestamp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, _}
import org.aisql.bigdata.base.framework.util.DataFrameReflactUtil
import org.aisql.bigdata.base.framework.hive.impl.SparkBaseHiveDaoImpl
import org.aisql.bigdata.base.gojira.test.dal.bean.FiGwAgrtExpressOrderEncryptTestBean
    
/**
  * Author: xiaohei
  * Date: 2019-10-16 09:58:10
  * CreateBy: @SparkHiveDaor
  *
  */
      
class FiGwAgrtExpressOrderEncryptTestSparkDao extends SparkBaseHiveDaoImpl[FiGwAgrtExpressOrderEncryptTestBean] {

  override val TABLE: String = "fi_gw_agrt_express_order_encrypt_test"
  override val DATABASE: String = "xy_jiangyuande"
    
  override val FULL_TABLENAME: String = s"$DATABASE.$TABLE"
  override val HDFS_PATH: String = s"/user/hive/warehouse/$DATABASE.db/$TABLE"
      
 /**
    * 读取hive数据时,将DadaFrame的Row转化为具体的bean对象
    *
    * @param df Row对象
    * @return 具体的bean对象
    **/
  override protected def transDf2Rdd(df: DataFrame)(implicit env: SparkSession): RDD[FiGwAgrtExpressOrderEncryptTestBean] = {
    df.rdd.map {
      row =>
        val bean = new FiGwAgrtExpressOrderEncryptTestBean
        val fields = DataFrameReflactUtil.getUsefulFields(classOf[FiGwAgrtExpressOrderEncryptTestBean]).map(f => (f.getName, f)).toMap
        row.schema.foreach {
          s =>
            fields.get(s.name).foreach {
              f =>
                f.setAccessible(true)
                f.set(bean, row.get(row.fieldIndex(s.name)))
            }
        }
        bean
    }
  }
    
 /**
    * 写入hive表时,将RDD转换为DataFrame
    *
    * @param rdd rdd对象
    * @return DataFrame对象
    **/
  override protected def transRdd2Df(rdd: RDD[FiGwAgrtExpressOrderEncryptTestBean])(implicit env: SparkSession): DataFrame = {
    val structs = DataFrameReflactUtil.getStructType(classOf[FiGwAgrtExpressOrderEncryptTestBean]).get
    val rowRdd = rdd.flatMap(r => DataFrameReflactUtil.generateRowValue(classOf[FiGwAgrtExpressOrderEncryptTestBean], r))
    env.createDataFrame(rowRdd, structs)
  }
    
}
