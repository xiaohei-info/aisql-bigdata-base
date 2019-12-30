package org.aisql.bigdata.base.framework.enums

/**
  * Author: xiaohei
  * Date: 2019/12/30
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
object MaxwellType extends Enumeration {
  type MaxwellTypeEnum = Value
  val INSERT = Value("insert")
  val UPDATE = Value("update")
  val DELETE = Value("delete")
}
