package org.aisql.bigdata.base.gojira.monster.sparkimpl

import org.aisql.bigdata.base.gojira.enum.MonsterType
import org.aisql.bigdata.base.gojira.enum.MonsterType.MonsterType
import org.aisql.bigdata.base.gojira.model.ClassModel
import org.aisql.bigdata.base.gojira.monster.Ancestor
import org.aisql.bigdata.base.util.DateUtil

/**
  * Author: xiaohei
  * Date: 2019/10/21
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
class SparkKafkaDaor(basePackage: String, whoami: String) extends Ancestor {

  private val bottomPkgName = "sparkimpl"

  override val monsterType: MonsterType = MonsterType.SPARK_KAFKA_DAO

  override protected var pkgName: String = s"package $basePackage.dal.dao.$bottomPkgName"

  override protected var impPkgs: String = _

  override protected var author: String =
    s"""
       |/**
       |  * Author: $whoami
       |  * Date: ${DateUtil.currTime}
       |  * CreateBy: @${this.getClass.getSimpleName}
       |  *
       |  */
      """.stripMargin

  /**
    * 类初始化设置
    *
    * 设置classModel相关字段
    **/
  override def init(): Unit = {
    val beanClsName = s"$baseClass${MonsterType.BEAN}"

  }


}
