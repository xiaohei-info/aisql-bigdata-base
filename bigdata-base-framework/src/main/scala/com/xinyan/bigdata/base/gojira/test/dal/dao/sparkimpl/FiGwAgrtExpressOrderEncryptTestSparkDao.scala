package com.xinyan.bigdata.base.gojira.test.dal.dao.sparkimpl

import java.sql.Timestamp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession, _}
import com.xinyan.bigdata.base.framework.hive.impl.SparkBaseHiveDaoImpl
import com.xinyan.bigdata.base.gojira.test.dal.bean.FiGwAgrtExpressOrderEncryptTestBean
    
/**
  * Author: xiaohei
  * Date: 2019-10-14 14:00:42
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
        val bean = new FiGwAgrtExpressOrderEncryptTestBean
        bean.aes_bank_id_card = row.getAs[String]("aes_bank_id_card")
        bean.aes_bank_card_no = row.getAs[String]("aes_bank_card_no")
        bean.aes_bank_mobile = row.getAs[String]("aes_bank_mobile")
        bean.aes_bank_card_name = row.getAs[String]("aes_bank_card_name")
        bean.id = row.getAs[Long]("id")
        bean.business_no = row.getAs[String]("business_no")
        bean.trade_no = row.getAs[String]("trade_no")
        bean.order_id = row.getAs[Long]("order_id")
        bean.member_id = row.getAs[Long]("member_id")
        bean.member_trans_id = row.getAs[String]("member_trans_id")
        bean.member_trans_date = row.getAs[java.sql.Timestamp]("member_trans_date")
        bean.pay_member_id = row.getAs[Long]("pay_member_id")
        bean.terminal_id = row.getAs[Long]("terminal_id")
        bean.order_state = row.getAs[String]("order_state")
        bean.order_money = row.getAs[java.math.BigDecimal]("order_money")
        bean.channel_id = row.getAs[Long]("channel_id")
        bean.sms_id = row.getAs[Long]("sms_id")
        bean.cgw_succ_flag = row.getAs[Byte]("cgw_succ_flag")
        bean.cgw_succ_time = row.getAs[java.sql.Timestamp]("cgw_succ_time")
        bean.cgw_succ_money = row.getAs[java.math.BigDecimal]("cgw_succ_money")
        bean.cgw_err_code = row.getAs[String]("cgw_err_code")
        bean.cgw_err_msg = row.getAs[String]("cgw_err_msg")
        bean.cm_flag = row.getAs[Byte]("cm_flag")
        bean.trans_fee = row.getAs[java.math.BigDecimal]("trans_fee")
        bean.fee_member_id = row.getAs[Long]("fee_member_id")
        bean.fee_acct_id = row.getAs[Long]("fee_acct_id")
        bean.fee_way = row.getAs[Byte]("fee_way")
        bean.order_type = row.getAs[Byte]("order_type")
        bean.err_code = row.getAs[String]("err_code")
        bean.err_msg = row.getAs[String]("err_msg")
        bean.version = row.getAs[String]("version")
        bean.sign_no = row.getAs[String]("sign_no")
        bean.pay_id = row.getAs[Long]("pay_id")
        bean.card_type = row.getAs[Byte]("card_type")
        bean.id_card_type = row.getAs[String]("id_card_type")
        bean.bank_card_no = row.getAs[String]("bank_card_no")
        bean.bank_card_name = row.getAs[String]("bank_card_name")
        bean.bank_mobile = row.getAs[String]("bank_mobile")
        bean.bank_id_card = row.getAs[String]("bank_id_card")
        bean.send_pay_succ_sms = row.getAs[Byte]("send_pay_succ_sms")
        bean.reserved = row.getAs[String]("reserved")
        bean.product_id = row.getAs[Long]("product_id")
        bean.function_id = row.getAs[Long]("function_id")
        bean.remark = row.getAs[String]("remark")
        bean.prepare_filed_one = row.getAs[String]("prepare_filed_one")
        bean.prepare_filed_two = row.getAs[String]("prepare_filed_two")
        bean.cur_version = row.getAs[Long]("cur_version")
        bean.update_time = row.getAs[java.sql.Timestamp]("update_time")
        bean.create_time = row.getAs[java.sql.Timestamp]("create_time")
        bean.addition_info = row.getAs[String]("addition_info")
        bean.reserved_addition_info = row.getAs[String]("reserved_addition_info")
        bean.pk_year = row.getAs[String]("pk_year")
        bean.pk_month = row.getAs[String]("pk_month")
        bean.pk_day = row.getAs[String]("pk_day")
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
    val structs = StructType(Seq[StructField](
        StructField("aes_bank_id_card", StringType, nullable = true),
        StructField("aes_bank_card_no", StringType, nullable = true),
        StructField("aes_bank_mobile", StringType, nullable = true),
        StructField("aes_bank_card_name", StringType, nullable = true),
        StructField("id", LongType, nullable = true),
        StructField("business_no", StringType, nullable = true),
        StructField("trade_no", StringType, nullable = true),
        StructField("order_id", LongType, nullable = true),
        StructField("member_id", LongType, nullable = true),
        StructField("member_trans_id", StringType, nullable = true),
        StructField("member_trans_date", TimestampType, nullable = true),
        StructField("pay_member_id", LongType, nullable = true),
        StructField("terminal_id", LongType, nullable = true),
        StructField("order_state", StringType, nullable = true),
        StructField("order_money", DataTypes.createDecimalType(), nullable = true),
        StructField("channel_id", LongType, nullable = true),
        StructField("sms_id", LongType, nullable = true),
        StructField("cgw_succ_flag", ByteType, nullable = true),
        StructField("cgw_succ_time", TimestampType, nullable = true),
        StructField("cgw_succ_money", DataTypes.createDecimalType(), nullable = true),
        StructField("cgw_err_code", StringType, nullable = true),
        StructField("cgw_err_msg", StringType, nullable = true),
        StructField("cm_flag", ByteType, nullable = true),
        StructField("trans_fee", DataTypes.createDecimalType(), nullable = true),
        StructField("fee_member_id", LongType, nullable = true),
        StructField("fee_acct_id", LongType, nullable = true),
        StructField("fee_way", ByteType, nullable = true),
        StructField("order_type", ByteType, nullable = true),
        StructField("err_code", StringType, nullable = true),
        StructField("err_msg", StringType, nullable = true),
        StructField("version", StringType, nullable = true),
        StructField("sign_no", StringType, nullable = true),
        StructField("pay_id", LongType, nullable = true),
        StructField("card_type", ByteType, nullable = true),
        StructField("id_card_type", StringType, nullable = true),
        StructField("bank_card_no", StringType, nullable = true),
        StructField("bank_card_name", StringType, nullable = true),
        StructField("bank_mobile", StringType, nullable = true),
        StructField("bank_id_card", StringType, nullable = true),
        StructField("send_pay_succ_sms", ByteType, nullable = true),
        StructField("reserved", StringType, nullable = true),
        StructField("product_id", LongType, nullable = true),
        StructField("function_id", LongType, nullable = true),
        StructField("remark", StringType, nullable = true),
        StructField("prepare_filed_one", StringType, nullable = true),
        StructField("prepare_filed_two", StringType, nullable = true),
        StructField("cur_version", LongType, nullable = true),
        StructField("update_time", TimestampType, nullable = true),
        StructField("create_time", TimestampType, nullable = true),
        StructField("addition_info", StringType, nullable = true),
        StructField("reserved_addition_info", StringType, nullable = true),
        StructField("pk_year", StringType, nullable = true),
        StructField("pk_month", StringType, nullable = true),
        StructField("pk_day", StringType, nullable = true)
    ))

    val rowRdd = rdd.map {
      r =>
        Row.fromSeq(r.toString.split(","))
    }
    env.createDataFrame(rowRdd, structs)
  }
    
}
