package org.aisql.bigdata.base.gojira.monster.flinkimpl

import org.aisql.bigdata.base.gojira.enum.MonsterType
import org.aisql.bigdata.base.gojira.enum.MonsterType.MonsterType
import org.aisql.bigdata.base.gojira.model.ClassModel
import org.aisql.bigdata.base.gojira.monster.Ancestor
import org.aisql.bigdata.base.util.StringUtil

/**
  * Author: xiaohei
  * Date: 2019/10/21
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
class FlinkKafkaDaor(basePackage: String, whoami: String) extends Ancestor(whoami) {

  override val monsterType: MonsterType = MonsterType.FLINK_KAFKA_DAO

  override val rootClass: String = "FlinkBaseKafkaDaoImpl"

  override val implPkg: String = "kafka.impl"

  override protected var pkgName: String = s"package $basePackage.dal.dao.flinkimpl"

  override protected var beanClassName: String = _

  override protected var classHeader: String = _

  override protected var impPkgs: String = _

  override protected var classModel: ClassModel = _


  /**
    * 类初始化设置
    *
    * 设置classModel相关字段
    **/
  override def init(): Unit = {
    beanClassName = s"$baseClass${MonsterType.BEAN}"

    classHeader =
      s"""
         |class $baseClass$monsterType extends $rootClass[${baseClass}Bean]""".stripMargin

    impPkgs =
      s"""
         |import com.alibaba.fastjson.JSON
         |import org.aisql.bigdata.base.util.JavaJsonUtil
         |import org.apache.flink.streaming.api.scala.DataStream
         |import $frameworkPackage.$implPkg.$rootClass
         |import $basePackage.dal.bean.$beanClassName
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
        |  override val BOOTSTRAP_SERVERS: String = ""
        |  override val ZK_HOST: String = ""
      """.stripMargin

    val transJson2Bean =
      s"""
         |override protected def transJson2Bean(jsonStream: DataStream[String]): DataStream[$beanClassName] = {
         |    jsonStream.map(x => JSON.parseObject(x, classOf[$beanClassName]))
         |  }
      """.stripMargin

    val transBean2Json =
      s"""
         |override protected def transBean2Json(beanStream: DataStream[$beanClassName]): DataStream[String] = {
         |    beanStream.map(x => JavaJsonUtil.toJSONString(x))
         |  }
      """.stripMargin

    classModel = new ClassModel(pkgName, classHeader)
    classModel.setImport(impPkgs)
    classModel.setAuthor(author)
    classModel.setFields(varFields)
    classModel.setFields(valFields)
    classModel.setMethods(transJson2Bean)
    classModel.setMethods(transBean2Json)
    logger.info(s"$monsterType class model done")
  }


}
