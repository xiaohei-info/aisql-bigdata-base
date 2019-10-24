package org.aisql.bigdata.base.gojira.monster.hive

import org.aisql.bigdata.base.gojira.enum.MonsterType
import org.aisql.bigdata.base.gojira.enum.MonsterType.MonsterType
import org.aisql.bigdata.base.gojira.model.ClassModel
import org.aisql.bigdata.base.gojira.monster.Ancestor
import org.aisql.bigdata.base.util.StringUtil

/**
  * Author: xiaohei
  * Date: 2019/9/15
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
class SparkHiveDaor(basePackage: String, whoami: String) extends Ancestor(whoami) {

  override val monsterType: MonsterType = MonsterType.SPARK_HIVE_DAO

  override val rootClass: String = "SparkBaseHiveDaoImpl"

  override val implPkg: String = "hive.impl"

  override protected var pkgName: String = s"package $basePackage.dal.dao.hive.sparkimpl"

  override protected var impPkgs: String = _

  override protected var beanClassName: String = _

  override protected var classHeader: String = _

  override protected var classModel: ClassModel = _

  /**
    * 类初始化设置
    * 相关属性设置完毕之后调用
    *
    * 设置classModel相关字段
    **/
  override def init(): Unit = {
    beanClassName= s"$baseClass${MonsterType.BEAN}"

    impPkgs =
      s"""
         |import java.sql.Timestamp
         |import org.apache.spark.rdd.RDD
         |import org.apache.spark.sql.{SparkSession, _}
         |import $frameworkPackage.util.DataFrameReflactUtil
         |import $frameworkPackage.$implPkg.$rootClass
         |import $basePackage.dal.bean.$beanClassName
    """.stripMargin

    classHeader =
      s"""
         |class $baseClass$monsterType extends $rootClass[${baseClass}Bean]""".stripMargin

    val valFields: String =
      """
        |  override val FULL_TABLENAME: String = s"$DATABASE.$TABLE"
        |  override val HDFS_PATH: String = s"/user/hive/warehouse/$DATABASE.db/$TABLE"
      """.stripMargin

    val varFields: String =
      s"""
         |  override val TABLE: String = "${StringUtil.camel2under(baseClass)}"
         |  override val DATABASE: String = "$database"
    """.stripMargin

    val transDf2Rdd: String =
      s"""
         | /**
         |    * 读取hive数据时,将DadaFrame的Row转化为具体的bean对象
         |    *
         |    * @param df Row对象
         |    * @return 具体的bean对象
         |    **/
         |  override protected def transDf2Rdd(df: DataFrame)(implicit env: SparkSession): RDD[$beanClassName] = {
         |    df.rdd.map {
         |      row =>
         |        DataFrameReflactUtil.generatePojoValue(classOf[$beanClassName], row).asInstanceOf[$beanClassName]
         |    }
         |  }
    """.stripMargin

    val transRdd2Df: String =
      s"""
         | /**
         |    * 写入hive表时,将RDD转换为DataFrame
         |    *
         |    * @param rdd rdd对象
         |    * @return DataFrame对象
         |    **/
         |  override protected def transRdd2Df(rdd: RDD[$beanClassName])(implicit env: SparkSession): DataFrame = {
         |    val structs = DataFrameReflactUtil.getStructType(classOf[$beanClassName]).get
         |    val rowRdd = rdd.flatMap(r => DataFrameReflactUtil.generateRowValue(classOf[$beanClassName], r))
         |    env.createDataFrame(rowRdd, structs)
         |  }
    """.stripMargin

    classModel = new ClassModel(pkgName, classHeader)
    classModel.setImport(impPkgs)
    classModel.setAuthor(author)
    classModel.setFields(varFields)
    classModel.setFields(valFields)
    classModel.setMethods(transDf2Rdd)
    classModel.setMethods(transRdd2Df)
    logger.info(s"$monsterType class model done")
  }
}
