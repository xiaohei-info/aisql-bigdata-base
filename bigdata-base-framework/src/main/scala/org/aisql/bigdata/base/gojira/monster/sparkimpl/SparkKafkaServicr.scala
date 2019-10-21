package org.aisql.bigdata.base.gojira.monster.sparkimpl

import org.aisql.bigdata.base.gojira.enum.MonsterType
import org.aisql.bigdata.base.gojira.enum.MonsterType.MonsterType
import org.aisql.bigdata.base.gojira.monster.Ancestor

/**
  * Author: xiaohei
  * Date: 2019/10/21
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
class SparkKafkaServicr(basePackage: String, whoami: String) extends Ancestor {

  override val monsterType: MonsterType =  MonsterType.SPARK_KAFKA_DAO

  /**
    * 类初始化设置
    *
    * 设置classModel相关字段
    **/
  override def init(): Unit = ???

  override protected var pkgName: String = _
  override protected var impPkgs: String = _
  override protected var author: String = _
}
