package org.aisql.bigdata.base.gojira.test.service.sparkimpl

import org.aisql.bigdata.base.framework.hive.BaseHiveDao
import org.aisql.bigdata.base.framework.hive.BaseHiveService
import org.aisql.bigdata.base.gojira.test.dal.bean.FiGwAgrtExpressOrderEncryptTestBean
import org.aisql.bigdata.base.gojira.test.dal.dao.sparkimpl.FiGwAgrtExpressOrderEncryptTestSparkDao

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
    
/**
  * Author: xiaohei
  * Date: 2019-10-16 09:58:10
  * CreateBy: @SparkHiveServicr
  *
  */
      
class FiGwAgrtExpressOrderEncryptTestSparkService extends BaseHiveService[SparkSession, RDD[FiGwAgrtExpressOrderEncryptTestBean]] {

  protected override val dao: BaseHiveDao[SparkSession, RDD[FiGwAgrtExpressOrderEncryptTestBean]] = new FiGwAgrtExpressOrderEncryptTestSparkDao
    
}
