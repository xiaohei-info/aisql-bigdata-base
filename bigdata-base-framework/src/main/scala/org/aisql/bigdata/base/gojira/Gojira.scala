package org.aisql.bigdata.base.gojira

import org.aisql.bigdata.base.gojira.monster.sparkimpl.{SparkHiveDaor, SparkHiveServicr}
import org.aisql.bigdata.base.gojira.monster.{Ancestor, Beanr}
import org.aisql.bigdata.base.gojira.enum.EngineType.EngineType
import org.aisql.bigdata.base.gojira.enum.{EngineType, MonsterType}
import org.aisql.bigdata.base.gojira.model.{FieldMeta, TableSchema}
import org.aisql.bigdata.base.java.ZipCompress
import org.aisql.bigdata.base.util.{FileUtil, HiveUtil, StringUtil}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * Author: xiaohei
  * Date: 2019/9/11
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
class Gojira(savePath: String,
             projectName: String,
             projectPkgName: String,
             whoami: String) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  logger.info(s"savePath: $savePath, projectName: $projectName, projectPkgName: $projectPkgName, whoami: $whoami")

  private val allMonsters: Seq[Ancestor] = Seq[Ancestor](
    new Beanr(projectPkgName, whoami),
    new SparkHiveDaor(projectPkgName, whoami),
    new SparkHiveServicr(projectPkgName, whoami)
  )

  logger.info("all monsters already init")

  private var monsters = allMonsters

  def setMonster(engineType: EngineType) = {
    logger.info(s"set monster engine type to $engineType")
    monsters = if (engineType == EngineType.ALL) allMonsters
    else allMonsters.filter(a => a.monsterType == MonsterType.BEAN || a.monsterType.toString.contains(engineType.toString))
    logger.info(s"monsters: ${monsters.map(_.getClass.getSimpleName).mkString(",")}")
  }

  private var schema: Seq[TableSchema] = Seq.empty[TableSchema]

  def setTable(tableNames: Seq[String], spark: SparkSession) = {
    logger.info("set table connect to hive and init schemas")
    schema = tableNames.map {
      tableName =>
        val baseClass: String = StringUtil.under2camel(tableName.split("\\.").last)
        val fieldMeta: Seq[FieldMeta] = HiveUtil.getScheme(spark, tableName).map(x => FieldMeta(x._1, x._2, x._3))
        logger.info(s"HiveUtil.getScheme --> $baseClass get ${fieldMeta.size} fields")
        TableSchema(tableName, baseClass, fieldMeta)
    }
    logger.info("schemas init finished")
  }

  def setSchema(tablesSchema: Seq[TableSchema]) = {
    logger.info(s"get test tableSchema, num of tables: ${tablesSchema.size}")
    schema = tablesSchema
  }

  private val projectPath = savePath + "/" + projectName

  def printBean(): Unit = {
    printSchema("Bean")
  }

  def printDao(): Unit = {
    printSchema("Dao")
  }

  def printService(): Unit = {
    printSchema("Service")
  }

  def print(): Unit = {
    printSchema("")
  }

  def save(): Unit = {
    if (!checkDir) return
    schema.foreach {
      tbs =>
        logger.info(s"start ${tbs.tableName} job, total fields: ${tbs.fieldsMeta.size}")
        monsters.foreach {
          monster =>
            logger.info(s"monster ${monster.monsterType} aoaoao~~~")
            monster.database = tbs.tableName.split("\\.").head
            monster.baseClass = tbs.baseClass
            monster.fieldMeta = tbs.fieldsMeta
            monster.init()

            //获取项目名到文件名之间的路径名称
            val dirName = monster.toString.split("\n").head.split(projectPkgName).last.replace(".", "/")
            val fileName = s"${tbs.baseClass}${monster.monsterType}.scala"
            logger.info(s"${monster.monsterType} save name: $projectPath$dirName/$fileName")
            FileUtil.saveFile(Seq[String](monster.toString), s"$projectPath$dirName/$fileName")
            logger.info("save done")
        }
    }

    logger.info("all table and monsters done, start zip compress")
    if (FileUtil.isExists(s"$savePath/$projectName")) {
      val zip = new ZipCompress(s"$savePath/$projectName.zip", s"$savePath/$projectName")
      zip.zip()
      logger.info("zip done, delete files")
      FileUtil.deleteFiles(s"$savePath/$projectName")
    } else {
      logger.error(s"$savePath/$projectName donen't exists, please set table or schema for gojira.")
    }
    logger.info("job finished, gojira go home now")
  }

  private def printSchema(monsterTypeStr: String): Unit = {
    val currMonster = if (monsterTypeStr == "") monsters else monsters.filter(_.monsterType.toString.contains(monsterTypeStr))
    schema.foreach {
      tbs =>
        currMonster.foreach {
          monster =>
            monster.database = tbs.tableName.split("\\.").head
            monster.baseClass = tbs.baseClass
            monster.fieldMeta = tbs.fieldsMeta
            monster.init()
            println(monster.toString)
        }
    }
  }

  private def checkDir: Boolean = {
    if (!FileUtil.isExists(savePath)) {
      logger.error(s"$savePath path doesn't exists!")
      return false
    }
    true
  }

}
