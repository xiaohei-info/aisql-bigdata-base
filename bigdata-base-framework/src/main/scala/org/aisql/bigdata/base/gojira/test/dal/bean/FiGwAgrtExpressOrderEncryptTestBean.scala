package org.aisql.bigdata.base.gojira.test.dal.bean

import java.sql.Timestamp
import java.sql.Date
import com.alibaba.fastjson.JSONObject
    
/**
  * Author: xiaohei
  * Date: 2019-10-16 09:58:10
  * CreateBy: @Beanr
  *
  */
      
class FiGwAgrtExpressOrderEncryptTestBean extends Serializable {

  /**
    * 
    **/
  var aes_bank_id_card:String = _
  
  /**
    * 
    **/
  var aes_bank_card_no:String = _
  
  /**
    * 
    **/
  var aes_bank_mobile:String = _
  
  /**
    * 
    **/
  var aes_bank_card_name:String = _
  
  /**
    * 自增主键
    **/
  var id:java.lang.Long = _
  
  /**
    * 业务流水号
    **/
  var business_no:String = _
  
  /**
    * 宝付交易号
    **/
  var trade_no:String = _
  
  /**
    * 宝付订单号
    **/
  var order_id:java.lang.Long = _
  
  /**
    * 商户号
    **/
  var member_id:java.lang.Long = _
  
  /**
    * 商户订单号
    **/
  var member_trans_id:String = _
  
  /**
    * 商户交易时间
    **/
  var member_trans_date:java.sql.Timestamp = _
  
  /**
    * 
    **/
  var pay_member_id:java.lang.Long = _
  
  /**
    * 终端号
    **/
  var terminal_id:java.lang.Long = _
  
  /**
    * 订单状态(I:初始，F:失败，S:成功)
    **/
  var order_state:String = _
  
  /**
    * 订单金额
    **/
  var order_money:java.math.BigDecimal = _
  
  /**
    * 渠道号
    **/
  var channel_id:java.lang.Long = _
  
  /**
    * 短信ID
    **/
  var sms_id:java.lang.Long = _
  
  /**
    * 渠道成功标识
    **/
  var cgw_succ_flag:java.lang.Byte = _
  
  /**
    * 渠道成功时间
    **/
  var cgw_succ_time:java.sql.Timestamp = _
  
  /**
    * 渠道成功金额
    **/
  var cgw_succ_money:java.math.BigDecimal = _
  
  /**
    * 渠道错误码
    **/
  var cgw_err_code:String = _
  
  /**
    * 渠道错误描述
    **/
  var cgw_err_msg:String = _
  
  /**
    * 通知清算标识
    **/
  var cm_flag:java.lang.Byte = _
  
  /**
    * 手续费
    **/
  var trans_fee:java.math.BigDecimal = _
  
  /**
    * 
    **/
  var fee_member_id:java.lang.Long = _
  
  /**
    * 手续费账户
    **/
  var fee_acct_id:java.lang.Long = _
  
  /**
    * 手续费方式
    **/
  var fee_way:java.lang.Byte = _
  
  /**
    * 订单类型
    **/
  var order_type:java.lang.Byte = _
  
  /**
    * 错误码
    **/
  var err_code:String = _
  
  /**
    * 错误描述
    **/
  var err_msg:String = _
  
  /**
    * 接口版本号
    **/
  var version:String = _
  
  /**
    * 签约ID
    **/
  var sign_no:String = _
  
  /**
    * 支付方式
    **/
  var pay_id:java.lang.Long = _
  
  /**
    * 卡类型
    **/
  var card_type:java.lang.Byte = _
  
  /**
    * 证件类型
    **/
  var id_card_type:String = _
  
  /**
    * 银行卡号
    **/
  var bank_card_no:String = _
  
  /**
    * 银行卡户名
    **/
  var bank_card_name:String = _
  
  /**
    * 银行预留手机号
    **/
  var bank_mobile:String = _
  
  /**
    * 银行卡开户证件号
    **/
  var bank_id_card:String = _
  
  /**
    * 发送支付成功短信(0:不发、1：发)
    **/
  var send_pay_succ_sms:java.lang.Byte = _
  
  /**
    * 保留域
    **/
  var reserved:String = _
  
  /**
    * 产品ID
    **/
  var product_id:java.lang.Long = _
  
  /**
    * 功能ID
    **/
  var function_id:java.lang.Long = _
  
  /**
    * 备注
    **/
  var remark:String = _
  
  /**
    * 预备字段1
    **/
  var prepare_filed_one:String = _
  
  /**
    * 预备字段2
    **/
  var prepare_filed_two:String = _
  
  /**
    * 更新序号
    **/
  var cur_version:java.lang.Long = _
  
  /**
    * 更新时间
    **/
  var update_time:java.sql.Timestamp = _
  
  /**
    * 创建时间
    **/
  var create_time:java.sql.Timestamp = _
  
  /**
    * 
    **/
  var addition_info:String = _
  
  /**
    * 
    **/
  var reserved_addition_info:String = _
  
  /**
    * 
    **/
  var pk_year:String = _
  
  /**
    * 
    **/
  var pk_month:String = _
  
  /**
    * 
    **/
  var pk_day:String = _
  
  override def toString = {
    s"$aes_bank_id_card,$aes_bank_card_no,$aes_bank_mobile,$aes_bank_card_name,$id,$business_no,$trade_no,$order_id,$member_id,$member_trans_id,$member_trans_date,$pay_member_id,$terminal_id,$order_state,$order_money,$channel_id,$sms_id,$cgw_succ_flag,$cgw_succ_time,$cgw_succ_money,$cgw_err_code,$cgw_err_msg,$cm_flag,$trans_fee,$fee_member_id,$fee_acct_id,$fee_way,$order_type,$err_code,$err_msg,$version,$sign_no,$pay_id,$card_type,$id_card_type,$bank_card_no,$bank_card_name,$bank_mobile,$bank_id_card,$send_pay_succ_sms,$reserved,$product_id,$function_id,$remark,$prepare_filed_one,$prepare_filed_two,$cur_version,$update_time,$create_time,$addition_info,$reserved_addition_info,$pk_year,$pk_month,$pk_day"
  }
    
  def toJSONString = {
    val json = new JSONObject()
    json.put("aes_bank_id_card", aes_bank_id_card)
    json.put("aes_bank_card_no", aes_bank_card_no)
    json.put("aes_bank_mobile", aes_bank_mobile)
    json.put("aes_bank_card_name", aes_bank_card_name)
    json.put("id", id)
    json.put("business_no", business_no)
    json.put("trade_no", trade_no)
    json.put("order_id", order_id)
    json.put("member_id", member_id)
    json.put("member_trans_id", member_trans_id)
    json.put("member_trans_date", member_trans_date)
    json.put("pay_member_id", pay_member_id)
    json.put("terminal_id", terminal_id)
    json.put("order_state", order_state)
    json.put("order_money", order_money)
    json.put("channel_id", channel_id)
    json.put("sms_id", sms_id)
    json.put("cgw_succ_flag", cgw_succ_flag)
    json.put("cgw_succ_time", cgw_succ_time)
    json.put("cgw_succ_money", cgw_succ_money)
    json.put("cgw_err_code", cgw_err_code)
    json.put("cgw_err_msg", cgw_err_msg)
    json.put("cm_flag", cm_flag)
    json.put("trans_fee", trans_fee)
    json.put("fee_member_id", fee_member_id)
    json.put("fee_acct_id", fee_acct_id)
    json.put("fee_way", fee_way)
    json.put("order_type", order_type)
    json.put("err_code", err_code)
    json.put("err_msg", err_msg)
    json.put("version", version)
    json.put("sign_no", sign_no)
    json.put("pay_id", pay_id)
    json.put("card_type", card_type)
    json.put("id_card_type", id_card_type)
    json.put("bank_card_no", bank_card_no)
    json.put("bank_card_name", bank_card_name)
    json.put("bank_mobile", bank_mobile)
    json.put("bank_id_card", bank_id_card)
    json.put("send_pay_succ_sms", send_pay_succ_sms)
    json.put("reserved", reserved)
    json.put("product_id", product_id)
    json.put("function_id", function_id)
    json.put("remark", remark)
    json.put("prepare_filed_one", prepare_filed_one)
    json.put("prepare_filed_two", prepare_filed_two)
    json.put("cur_version", cur_version)
    json.put("update_time", update_time)
    json.put("create_time", create_time)
    json.put("addition_info", addition_info)
    json.put("reserved_addition_info", reserved_addition_info)
    json.put("pk_year", pk_year)
    json.put("pk_month", pk_month)
    json.put("pk_day", pk_day)
    json.toJSONString
  }
     
}
