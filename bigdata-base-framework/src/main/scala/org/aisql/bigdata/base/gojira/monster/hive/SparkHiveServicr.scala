package org.aisql.bigdata.base.gojira.monster.hive

import org.aisql.bigdata.base.gojira.enum.MonsterType
import org.aisql.bigdata.base.gojira.enum.MonsterType.MonsterType
import org.aisql.bigdata.base.gojira.model.ClassModel
import org.aisql.bigdata.base.gojira.monster.Ancestor

/**
  * Author: xiaohei
  * Date: 2019/9/15
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
class SparkHiveServicr(basePackage: String, whoami: String) extends Ancestor(whoami) {

  override val monsterType: MonsterType = MonsterType.SPARK_HIVE_SERVICE

  override val rootClass: String = "BaseHiveService"

  override val implPkg: String = "hive"

  override protected var pkgName: String = s"package $basePackage.service.hive.sparkimpl"

  override protected var impPkgs: String = _

  override protected var beanClassName: String = _

  override protected var classHeader: String = _

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
         |class $baseClass$monsterType extends $rootClass[SparkSession, RDD[${baseClass}Bean]] with Traceable[${baseClass}Bean]""".stripMargin

    val daoClassName = s"$baseClass${MonsterType.SPARK_HIVE_DAO}"

    impPkgs =
      s"""
         |import $frameworkPackage.Traceable
         |import $frameworkPackage.$implPkg.{$rootClass, BaseHiveDao}
         |import $basePackage.dal.bean.$beanClassName
         |import $basePackage.dal.dao.hive.sparkimpl.$daoClassName

         |import org.apache.spark.rdd.RDD
         |import org.apache.spark.sql.SparkSession
    """.stripMargin

    val fields: String =
      s"""
         |  protected override val dao: BaseHiveDao[SparkSession, RDD[$beanClassName]] = new $daoClassName
    """.stripMargin


    classModel = new ClassModel(pkgName, classHeader)
    classModel.setImport(impPkgs)
    classModel.setAuthor(author)
    classModel.setFields(fields)
    logger.info(s"$monsterType class model done")
  }


}
