package com.xinyan.bigdata.base.gojira

import com.xinyan.bigdata.base.gojira.enum.EngineType
import com.xinyan.bigdata.base.gojira.test.service.sparkimpl.FiGwAgrtExpressOrderEncryptTestSparkService

/**
  * Author: xiaohei
  * Date: 2019/10/12
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
object Test {
  def main(args: Array[String]) {
    val fieldMeta = Seq[(String, String, String)](
      ("aes_bank_id_card","String",""),
      ("aes_bank_card_no","String",""),
      ("aes_bank_mobile","String",""),
      ("aes_bank_card_name","String",""),
      ("id","Long","自增主键"),
      ("business_no","String","业务流水号"),
      ("trade_no","String","宝付交易号"),
      ("order_id","Long","宝付订单号"),
      ("member_id","Long","商户号"),
      ("member_trans_id","String","商户订单号"),
      ("member_trans_date","java.sql.Timestamp","商户交易时间"),
      ("pay_member_id","Long",""),
      ("terminal_id","Long","终端号"),
      ("order_state","String","订单状态(I:初始，F:失败，S:成功)"),
      ("order_money","java.math.BigDecimal","订单金额"),
      ("channel_id","Long","渠道号"),
      ("sms_id","Long","短信ID"),
      ("cgw_succ_flag","Byte","渠道成功标识"),
      ("cgw_succ_time","java.sql.Timestamp","渠道成功时间"),
      ("cgw_succ_money","java.math.BigDecimal","渠道成功金额"),
      ("cgw_err_code","String","渠道错误码"),
      ("cgw_err_msg","String","渠道错误描述"),
      ("cm_flag","Byte","通知清算标识"),
      ("trans_fee","java.math.BigDecimal","手续费"),
      ("fee_member_id","Long",""),
      ("fee_acct_id","Long","手续费账户"),
      ("fee_way","Byte","手续费方式"),
      ("order_type","Byte","订单类型"),
      ("err_code","String","错误码"),
      ("err_msg","String","错误描述"),
      ("version","String","接口版本号"),
      ("sign_no","String","签约ID"),
      ("pay_id","Long","支付方式"),
      ("card_type","Byte","卡类型"),
      ("id_card_type","String","证件类型"),
      ("bank_card_no","String","银行卡号"),
      ("bank_card_name","String","银行卡户名"),
      ("bank_mobile","String","银行预留手机号"),
      ("bank_id_card","String","银行卡开户证件号"),
      ("send_pay_succ_sms","Byte","发送支付成功短信(0:不发、1：发)"),
      ("reserved","String","保留域"),
      ("product_id","Long","产品ID"),
      ("function_id","Long","功能ID"),
      ("remark","String","备注"),
      ("prepare_filed_one","String","预备字段1"),
      ("prepare_filed_two","String","预备字段2"),
      ("cur_version","Long","更新序号"),
      ("update_time","java.sql.Timestamp","更新时间"),
      ("create_time","java.sql.Timestamp","创建时间"),
      ("addition_info","String",""),
      ("reserved_addition_info","String",""),
      ("pk_year","String",""),
      ("pk_month","String",""),
      ("pk_day","String","")
    )
    val tableName = "xy_jiangyuande.fi_gw_agrt_express_order_encrypt_test"
    val baseClass: String = "FiGwAgrtExpressOrderEncryptTest"
    val whoami = "xiaohei"

    val gojira = new Gojira(
      "/Users/xiaohei/Downloads/tmp/test",
      "gojira-test",
      "com.xinyan.bigdata.base.gojira.test",
      whoami
    )

    gojira.setActor(EngineType.SPARK)
    gojira.setSchema(Seq((tableName, baseClass, fieldMeta)))

    gojira.save()
    val service = new FiGwAgrtExpressOrderEncryptTestSparkService
    service.select()
  }
}
