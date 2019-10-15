package org.aisql.bigdata.base.gojira

import org.aisql.bigdata.base.gojira.monster.sparkimpl.{SparkDaor, SparkServicr}
import org.aisql.bigdata.base.gojira.monster.{Ancestor, Beanr}
import org.aisql.bigdata.base.gojira.enum.EngineType.EngineType
import org.aisql.bigdata.base.gojira.enum.{MonsterType, EngineType}
import org.aisql.bigdata.base.java.ZipCompress
import org.aisql.bigdata.base.util.{FileUtil, HiveUtil, StringUtil}
import org.apache.spark.sql.SparkSession

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

  private val allMonsters: Seq[Ancestor] = Seq[Ancestor](
    new Beanr(projectPkgName, whoami),
    new SparkDaor(projectPkgName, whoami),
    new SparkServicr(projectPkgName, whoami)
  )

  private var monsters = allMonsters

  def setMonster(engineType: EngineType) = {
    monsters = if (engineType == EngineType.ALL) allMonsters
    else allMonsters.filter(a => a.monsterType == MonsterType.BEAN || a.monsterType.toString.contains(engineType.toString))
  }

  private var schema: Seq[(String, String, Seq[(String, String, String)])] = Seq.empty[(String, String, Seq[(String, String, String)])]

  def setTable(tableNames: Seq[String], spark: SparkSession) = {
    schema = tableNames.map {
      tableName =>
        val baseClass: String = StringUtil.under2camel(tableName.split("\\.").last)
        val fieldMeta: Seq[(String, String, String)] = HiveUtil.getScheme(spark, tableName)
        (tableName, baseClass, fieldMeta)
    }
  }

  def setSchema(tableSchema: Seq[(String, String, Seq[(String, String, String)])]) = {
    schema = tableSchema
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
      case (tableName, baseClass, fieldMeta) =>
        monsters.foreach {
          monster =>
            monster.database = tableName.split("\\.").head
            monster.baseClass = baseClass
            monster.fieldMeta = fieldMeta
            monster.init()

            //获取项目名到文件名之间的路径名称
            val dirName = monster.toString.split("\n").head.split(projectPkgName).last.replace(".", "/")
            val fileName = s"$baseClass${monster.monsterType}.scala"
            FileUtil.saveFile(Seq[String](monster.toString), s"$projectPath/$dirName/$fileName")
        }
    }

    if (FileUtil.isExists(s"$savePath/$projectName")) {
      val zip = new ZipCompress(s"$savePath/$projectName.zip", s"$savePath/$projectName")
      zip.zip()
      FileUtil.deleteFiles(s"$savePath/$projectName")
    } else {
      println(s"$savePath/$projectName donen't exists, please set table or schema for gojira.")
    }
  }

  private def printSchema(monsterTypeStr: String): Unit = {
    val currMonster = if (monsterTypeStr == "") monsters else monsters.filter(_.monsterType.toString.contains(monsterTypeStr))
    schema.foreach {
      case (tableName, baseClass, fieldMeta) =>
        currMonster.foreach {
          monster =>
            monster.database = tableName.split("\\.").head
            monster.baseClass = baseClass
            monster.fieldMeta = fieldMeta
            monster.init()
            println(monster.toString)
        }
    }
  }

  private def checkDir: Boolean = {
    if (!FileUtil.isExists(savePath)) {
      println("path doesn't exists!")
      return false
    }
    true
  }

}
