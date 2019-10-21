package org.aisql.bigdata.base.gojira.monster.sparkimpl

import org.aisql.bigdata.base.gojira.enum.MonsterType
import org.aisql.bigdata.base.gojira.enum.MonsterType.MonsterType
import org.aisql.bigdata.base.gojira.model.ClassModel
import org.aisql.bigdata.base.gojira.monster.Ancestor
import org.aisql.bigdata.base.util.{DateUtil, StringUtil}

/**
  * Author: xiaohei
  * Date: 2019/10/21
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
class SparkKafkaDaor(basePackage: String, whoami: String) extends Ancestor {

  logger.info(s"${this.getClass.getSimpleName} init")

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

    impPkgs =
      s"""
         |import com.alibaba.fastjson.JSON
         |import org.aisql.bigdata.base.framework.kafka.impl.SparkBaseKafkaDaoImpl
         |import org.aisql.bigdata.base.util.JavaJsonUtil
         |import org.apache.spark.streaming.dstream.DStream
         |import $basePackage.dal.bean.$beanClsName
    """.stripMargin

    val varFields: String =
      s"""
         |  override val TABLE: String = "${StringUtil.camel2under(baseClass)}"
         |  override val DATABASE: String = "$database"
    """.stripMargin

    val valFields: String =
      """
        |  override val GROUP_ID: String = s"$DATABASE-$TABLE"
        |  override val TOPIC: String = s"$DATABASE-$TABLE"
        |  override val BOOTSTRAP_SERVERS: String = _
        |  override val ZK_HOST: String = _
      """.stripMargin

    val transJson2Bean =
      s"""
         | override protected def transJson2Bean(jsonStream: DStream[String]): DStream[$beanClsName] = {
         |    jsonStream.map(x => JSON.parseObject(x, classOf[$beanClsName]))
         |  }
      """.stripMargin

    val transBean2Json =
      s"""
         |override protected def transBean2Json(beanStream: DStream[$beanClsName]): DStream[String] = {
         |    beanStream.map(x => JavaJsonUtil.toJSONString(x))
         |  }
      """.stripMargin

    val clsHeader: String =
      s"""
         |class $baseClass$monsterType extends SparkBaseKafkaDaoImpl[${baseClass}Bean]""".stripMargin
    classModel = new ClassModel(pkgName, clsHeader)
    classModel.setImport(impPkgs)
    classModel.setAuthor(author)
    classModel.setFields(varFields)
    classModel.setFields(valFields)
    classModel.setMethods(transJson2Bean)
    classModel.setMethods(transBean2Json)
    logger.info(s"$monsterType class model done")
  }


}
