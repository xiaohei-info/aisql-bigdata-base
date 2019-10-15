package com.xinyan.bigdata.base.gojira.test.dal.dao.sparkimpl

import java.sql.Timestamp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession, _}
import com.xinyan.bigdata.base.framework.util.DataFrameReflactUtil
import com.xinyan.bigdata.base.framework.hive.impl.SparkBaseHiveDaoImpl
import com.xinyan.bigdata.base.gojira.test.dal.bean.FiGwAgrtExpressOrderEncryptTestBean
    
/**
  * Author: xiaohei
  * Date: 2019-10-15 17:02:07
  * CreateBy: @SparkDaor
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
        val allKeys = row.schema.map(_.toString.split("\(").last.split(",").head)
        val bean = new FiGwAgrtExpressOrderEncryptTestBean
        bean.aes_bank_id_card = if (allKeys.contains("aes_bank_id_card")) row.getAs[String]("aes_bank_id_card") else null
        bean.aes_bank_card_no = if (allKeys.contains("aes_bank_card_no")) row.getAs[String]("aes_bank_card_no") else null
        bean.aes_bank_mobile = if (allKeys.contains("aes_bank_mobile")) row.getAs[String]("aes_bank_mobile") else null
        bean.aes_bank_card_name = if (allKeys.contains("aes_bank_card_name")) row.getAs[String]("aes_bank_card_name") else null
        bean.id = if (allKeys.contains("id")) row.getAs[java.lang.Long]("id") else null
        bean.business_no = if (allKeys.contains("business_no")) row.getAs[String]("business_no") else null
        bean.trade_no = if (allKeys.contains("trade_no")) row.getAs[String]("trade_no") else null
        bean.order_id = if (allKeys.contains("order_id")) row.getAs[java.lang.Long]("order_id") else null
        bean.member_id = if (allKeys.contains("member_id")) row.getAs[java.lang.Long]("member_id") else null
        bean.member_trans_id = if (allKeys.contains("member_trans_id")) row.getAs[String]("member_trans_id") else null
        bean.member_trans_date = if (allKeys.contains("member_trans_date")) row.getAs[java.sql.Timestamp]("member_trans_date") else null
        bean.pay_member_id = if (allKeys.contains("pay_member_id")) row.getAs[java.lang.Long]("pay_member_id") else null
        bean.terminal_id = if (allKeys.contains("terminal_id")) row.getAs[java.lang.Long]("terminal_id") else null
        bean.order_state = if (allKeys.contains("order_state")) row.getAs[String]("order_state") else null
        bean.order_money = if (allKeys.contains("order_money")) row.getAs[java.math.BigDecimal]("order_money") else null
        bean.channel_id = if (allKeys.contains("channel_id")) row.getAs[java.lang.Long]("channel_id") else null
        bean.sms_id = if (allKeys.contains("sms_id")) row.getAs[java.lang.Long]("sms_id") else null
        bean.cgw_succ_flag = if (allKeys.contains("cgw_succ_flag")) row.getAs[java.lang.Byte]("cgw_succ_flag") else null
        bean.cgw_succ_time = if (allKeys.contains("cgw_succ_time")) row.getAs[java.sql.Timestamp]("cgw_succ_time") else null
        bean.cgw_succ_money = if (allKeys.contains("cgw_succ_money")) row.getAs[java.math.BigDecimal]("cgw_succ_money") else null
        bean.cgw_err_code = if (allKeys.contains("cgw_err_code")) row.getAs[String]("cgw_err_code") else null
        bean.cgw_err_msg = if (allKeys.contains("cgw_err_msg")) row.getAs[String]("cgw_err_msg") else null
        bean.cm_flag = if (allKeys.contains("cm_flag")) row.getAs[java.lang.Byte]("cm_flag") else null
        bean.trans_fee = if (allKeys.contains("trans_fee")) row.getAs[java.math.BigDecimal]("trans_fee") else null
        bean.fee_member_id = if (allKeys.contains("fee_member_id")) row.getAs[java.lang.Long]("fee_member_id") else null
        bean.fee_acct_id = if (allKeys.contains("fee_acct_id")) row.getAs[java.lang.Long]("fee_acct_id") else null
        bean.fee_way = if (allKeys.contains("fee_way")) row.getAs[java.lang.Byte]("fee_way") else null
        bean.order_type = if (allKeys.contains("order_type")) row.getAs[java.lang.Byte]("order_type") else null
        bean.err_code = if (allKeys.contains("err_code")) row.getAs[String]("err_code") else null
        bean.err_msg = if (allKeys.contains("err_msg")) row.getAs[String]("err_msg") else null
        bean.version = if (allKeys.contains("version")) row.getAs[String]("version") else null
        bean.sign_no = if (allKeys.contains("sign_no")) row.getAs[String]("sign_no") else null
        bean.pay_id = if (allKeys.contains("pay_id")) row.getAs[java.lang.Long]("pay_id") else null
        bean.card_type = if (allKeys.contains("card_type")) row.getAs[java.lang.Byte]("card_type") else null
        bean.id_card_type = if (allKeys.contains("id_card_type")) row.getAs[String]("id_card_type") else null
        bean.bank_card_no = if (allKeys.contains("bank_card_no")) row.getAs[String]("bank_card_no") else null
        bean.bank_card_name = if (allKeys.contains("bank_card_name")) row.getAs[String]("bank_card_name") else null
        bean.bank_mobile = if (allKeys.contains("bank_mobile")) row.getAs[String]("bank_mobile") else null
        bean.bank_id_card = if (allKeys.contains("bank_id_card")) row.getAs[String]("bank_id_card") else null
        bean.send_pay_succ_sms = if (allKeys.contains("send_pay_succ_sms")) row.getAs[java.lang.Byte]("send_pay_succ_sms") else null
        bean.reserved = if (allKeys.contains("reserved")) row.getAs[String]("reserved") else null
        bean.product_id = if (allKeys.contains("product_id")) row.getAs[java.lang.Long]("product_id") else null
        bean.function_id = if (allKeys.contains("function_id")) row.getAs[java.lang.Long]("function_id") else null
        bean.remark = if (allKeys.contains("remark")) row.getAs[String]("remark") else null
        bean.prepare_filed_one = if (allKeys.contains("prepare_filed_one")) row.getAs[String]("prepare_filed_one") else null
        bean.prepare_filed_two = if (allKeys.contains("prepare_filed_two")) row.getAs[String]("prepare_filed_two") else null
        bean.cur_version = if (allKeys.contains("cur_version")) row.getAs[java.lang.Long]("cur_version") else null
        bean.update_time = if (allKeys.contains("update_time")) row.getAs[java.sql.Timestamp]("update_time") else null
        bean.create_time = if (allKeys.contains("create_time")) row.getAs[java.sql.Timestamp]("create_time") else null
        bean.addition_info = if (allKeys.contains("addition_info")) row.getAs[String]("addition_info") else null
        bean.reserved_addition_info = if (allKeys.contains("reserved_addition_info")) row.getAs[String]("reserved_addition_info") else null
        bean.pk_year = if (allKeys.contains("pk_year")) row.getAs[String]("pk_year") else null
        bean.pk_month = if (allKeys.contains("pk_month")) row.getAs[String]("pk_month") else null
        bean.pk_day = if (allKeys.contains("pk_day")) row.getAs[String]("pk_day") else null
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
