package com.xinyan.bigdata.base.gojira.test.service.sparkimpl

import com.xinyan.bigdata.base.framework.hive.BaseHiveDao
import com.xinyan.bigdata.base.framework.hive.BaseHiveService
import com.xinyan.bigdata.base.gojira.test.dal.bean.FiGwAgrtExpressOrderEncryptTestBean
import com.xinyan.bigdata.base.gojira.test.dal.dao.sparkimpl.FiGwAgrtExpressOrderEncryptTestSparkDao

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
    
/**
  * Author: xiaohei
  * Date: 2019-10-15 17:02:07
  * CreateBy: @SparkServicr
  *
  */
      
class FiGwAgrtExpressOrderEncryptTestSparkService extends BaseHiveService[SparkSession, RDD[FiGwAgrtExpressOrderEncryptTestBean]] {

  protected override val dao: BaseHiveDao[SparkSession, RDD[FiGwAgrtExpressOrderEncryptTestBean]] = new FiGwAgrtExpressOrderEncryptTestSparkDao
    
}
