package org.aisql.bigdata.base.gojira

import org.aisql.bigdata.base.gojira.enum.EngineType.EngineType
import org.aisql.bigdata.base.gojira.enum.{EngineType, MonsterType}
import org.aisql.bigdata.base.gojira.model.{FieldMeta, TableSchema}
import org.aisql.bigdata.base.gojira.monster.cake.{Demor, Pomr}
import org.aisql.bigdata.base.gojira.monster.hive.{SparkHiveDaor, SparkHiveServicr}
import org.aisql.bigdata.base.gojira.monster.kafka.{FlinkKafkaDaor, FlinkKafkaServicr, SparkKafkaDaor, SparkKafkaServicr}
import org.aisql.bigdata.base.gojira.monster.{Ancestor, Beanr}
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
    new SparkHiveServicr(projectPkgName, whoami),
    new SparkKafkaDaor(projectPkgName, whoami),
    new SparkKafkaServicr(projectPkgName, whoami),
    new FlinkKafkaDaor(projectPkgName, whoami),
    new FlinkKafkaServicr(projectPkgName, whoami)
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

  def setTable(tableNames: Seq[String], spark: SparkSession, toCamel: Boolean = false) = {
    println("set table connect to hive and init schemas")
    schema = tableNames.map {
      tableName =>
        val baseClass: String = StringUtil.under2camel(tableName.split("\\.").last)
        val fieldMeta: Seq[FieldMeta] = HiveUtil.getScheme(spark, tableName, toCamel).map(x => FieldMeta(x._1, x._2, x._3))
        println(s"HiveUtil.getScheme --> $baseClass get ${fieldMeta.size} fields")
        TableSchema(tableName, baseClass, fieldMeta)
    }
    println("schemas init finished")
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

  def save(groupId: String, artifactId: String = projectName, version: String = "1.0-SNAPSHOT"): Unit = {
    if (!checkDir) return

    def prepareModule(modulePath: String): Unit = {
      val pom = new Pomr(groupId, artifactId, version)

      if (modulePath.contains("-context") || modulePath.contains("-server") || modulePath.contains("-api")) {
        mkModuleDir(modulePath)
        pom.setParent()
        if (modulePath.contains("-context")) {
          pom.setArtifactId("context")
          pom.setDependencies("context")
          pom.setBuild("context")
        } else if (modulePath.contains("-server")) {
          pom.setArtifactId("server")
          pom.setDependencies("server")
        } else {
          pom.setArtifactId("api")
        }
      } else {
        FileUtil.mkdir(projectPath)
        pom.setCoord("root")
        pom.setModule()
        pom.setProperties()
        pom.setDependencies("root")
        pom.setDependencyManagement()
        pom.setBuild("root")
      }
      FileUtil.saveFile(Seq[String](pom.toString), s"$modulePath/pom.xml")
    }

    //项目模块路径
    val contextPath = s"$projectPath/$projectName-context"
    val serverPath = s"$projectPath/$projectName-server"
    val apiPath = s"$projectPath/$projectName-api"

    prepareModule(projectPath)
    prepareModule(contextPath)
    prepareModule(serverPath)
    prepareModule(apiPath)

    //context demo
    val demor = new Demor(projectName, projectPkgName, whoami)
    val pkgPath = projectPkgName.replace(".", "/")
    FileUtil.mkdir(s"$contextPath/src/main/scala/$pkgPath/context")
    FileUtil.saveFile(Seq[String](demor.getFlinkDemo), s"$contextPath/src/main/scala/$pkgPath/demo/FlinkKafkaDemo.scala")
    FileUtil.saveFile(Seq[String](demor.getSparkDemo), s"$contextPath/src/main/scala/$pkgPath/demo/SparkKafkaDemo.scala")

    schema.foreach {
      tbs =>
        monsters.foreach {
          monster =>
            logger.info(s"monster ${monster.monsterType} aoaoao~~~")
            monster.database = tbs.tableName.split("\\.").head
            monster.baseClass = tbs.baseClass
            monster.fieldMeta = tbs.fieldsMeta
            monster.init()

            //获取项目名到文件名之间的路径名称
            //            val dirName = monster.toString.split("\n").head.split(projectPkgName).last.replace(".", "/")
            val dirName = monster.toString.split("\n").head.split(" ").last.replace(".", "/")
            val fileName = s"${tbs.baseClass}${monster.monsterType}.scala"
            val finalPath = s"$serverPath/src/main/scala/$dirName/$fileName"
            logger.info(s"${monster.monsterType} save name: $finalPath")
            FileUtil.saveFile(Seq[String](monster.toString), finalPath)
            logger.info("save done")
        }
    }

    logger.info("all table and monsters done, start zip compress")
    if (FileUtil.isExists(projectPath)) {
      val zip = new ZipCompress(s"$projectPath.zip", projectPath)
      zip.zip()
      logger.info("zip done, delete files")
      FileUtil.deleteFiles(projectPath)
    } else {
      logger.error(s"$projectPath donen't exists, please set table or schema for gojira.")
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

  private def mkModuleDir(modulePath: String): Unit = {
    //创建代码路径
    FileUtil.mkdir(s"$modulePath/src/main/scala/${projectPkgName.replace(".", "/")}")
    FileUtil.mkdir(s"$modulePath/src/main/resources")
    FileUtil.mkdir(s"$modulePath/src/test/scala")
  }

}
