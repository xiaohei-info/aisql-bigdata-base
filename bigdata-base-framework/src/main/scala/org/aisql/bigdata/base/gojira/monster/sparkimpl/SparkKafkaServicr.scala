package org.aisql.bigdata.base.gojira.monster.sparkimpl

import org.aisql.bigdata.base.gojira.enum.MonsterType
import org.aisql.bigdata.base.gojira.enum.MonsterType.MonsterType
import org.aisql.bigdata.base.gojira.model.ClassModel
import org.aisql.bigdata.base.gojira.monster.Ancestor

/**
  * Author: xiaohei
  * Date: 2019/10/21
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
class SparkKafkaServicr(basePackage: String, whoami: String) extends Ancestor(whoami) {

  override val monsterType: MonsterType = MonsterType.SPARK_KAFKA_SERVICE

  override val rootClass: String = "BaseKafkaService"

  override val implPkg: String = "kafka"

  override protected var pkgName: String = s"package $basePackage.service.sparkimpl"

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
         |class $baseClass$monsterType extends $rootClass[StreamingContext, DStream[${baseClass}Bean]]""".stripMargin

    val daoClassName = s"$baseClass${MonsterType.SPARK_KAFKA_DAO}"

    impPkgs =
      s"""
         |import org.apache.spark.streaming.StreamingContext
         |import org.apache.spark.streaming.dstream.DStream
         |$frameworkPackage.$implPkg.{$rootClass, BaseKafkaDao}
      """.stripMargin

    val fields: String =
      s"""
         |  protected override val dao: BaseHiveDao[SparkSession, DStream[$beanClassName]] = new $daoClassName
    """.stripMargin

    classModel = new ClassModel(pkgName, classHeader)
    classModel.setImport(impPkgs)
    classModel.setAuthor(author)
    classModel.setFields(fields)
    logger.info(s"$monsterType class model done")
  }


}
