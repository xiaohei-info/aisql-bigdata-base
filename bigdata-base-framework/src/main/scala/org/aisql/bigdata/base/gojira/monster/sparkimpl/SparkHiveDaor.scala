package org.aisql.bigdata.base.gojira.monster.sparkimpl

import org.aisql.bigdata.base.gojira.monster.Ancestor
import org.aisql.bigdata.base.gojira.enum.MonsterType
import org.aisql.bigdata.base.gojira.enum.MonsterType.MonsterType
import org.aisql.bigdata.base.gojira.model.ClassModel
import org.aisql.bigdata.base.util.{DateUtil, StringUtil}

/**
  * Author: xiaohei
  * Date: 2019/9/15
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
class SparkHiveDaor(basePackage: String, whoami: String) extends Ancestor {

  logger.info(s"${this.getClass.getSimpleName} init")

  private val bottomPkgName = "sparkimpl"

  override val monsterType: MonsterType = MonsterType.SPARK_DAO

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
    * 相关属性设置完毕之后调用
    *
    * 设置classModel相关字段
    **/
  override def init(): Unit = {
    val beanClsName = s"${baseClass}Bean"

    impPkgs =
      s"""
         |import java.sql.Timestamp
         |import org.apache.spark.rdd.RDD
         |import org.apache.spark.sql.{SparkSession, _}
         |import org.aisql.bigdata.base.framework.util.DataFrameReflactUtil
         |import org.aisql.bigdata.base.framework.hive.impl.SparkBaseHiveDaoImpl
         |import $basePackage.dal.bean.$beanClsName
    """.stripMargin

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
         |  override protected def transDf2Rdd(df: DataFrame)(implicit env: SparkSession): RDD[$beanClsName] = {
         |    df.rdd.map {
         |      row =>
         |        val bean = new $beanClsName
         |        val fields = DataFrameReflactUtil.getUsefulFields(classOf[$beanClsName]).map(f => (f.getName, f)).toMap
         |        row.schema.foreach {
         |          s =>
         |            fields.get(s.name).foreach {
         |              f =>
         |                f.setAccessible(true)
         |                f.set(bean, row.get(row.fieldIndex(s.name)))
         |            }
         |        }
         |        bean
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
         |  override protected def transRdd2Df(rdd: RDD[$beanClsName])(implicit env: SparkSession): DataFrame = {
         |    val structs = DataFrameReflactUtil.getStructType(classOf[$beanClsName]).get
         |    val rowRdd = rdd.flatMap(r => DataFrameReflactUtil.generateRowValue(classOf[$beanClsName], r))
         |    env.createDataFrame(rowRdd, structs)
         |  }
    """.stripMargin

    classModel = initClassModel
    classModel.setImport(impPkgs)
    classModel.setAuthor(author)
    classModel.setFields(varFields)
    classModel.setFields(valFields)
    classModel.setMethods(transDf2Rdd)
    classModel.setMethods(transRdd2Df)
    logger.info(s"$monsterType class model done")
  }

  private def initClassModel: ClassModel = {
    //todo: SparkBaseHiveDaoImpl 名称需要与 org.aisql.bigdata.base.framework.hive.impl.SparkBaseHiveDaoImpl 保持一致
    val clsHeader: String =
      s"""
         |class $baseClass$monsterType extends SparkBaseHiveDaoImpl[${baseClass}Bean]""".stripMargin
    new ClassModel(pkgName, clsHeader)
  }


}
