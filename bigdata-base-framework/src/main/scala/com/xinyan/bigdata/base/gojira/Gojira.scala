package com.xinyan.bigdata.base.gojira

import com.xinyan.bigdata.base.gojira.actor.sparkimpl.{SparkDaor, SparkServicr}
import com.xinyan.bigdata.base.gojira.actor.{Ancestor, Beanr}
import com.xinyan.bigdata.base.gojira.enum.{ActorType, EngineType}
import com.xinyan.bigdata.base.gojira.enum.EngineType.EngineType
import com.xinyan.bigdata.base.java.ZipCompress
import com.xinyan.bigdata.base.util.{FileUtil, HiveUtil, StringUtil}
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

  private val allActors: Seq[Ancestor] = Seq[Ancestor](
    new Beanr(projectPkgName, whoami),
    new SparkDaor(projectPkgName, whoami),
    new SparkServicr(projectPkgName, whoami)
  )

  private var actors = allActors

  def setActor(engineType: EngineType) = {
    actors = if (engineType == EngineType.ALL) allActors
    else allActors.filter(a => a.actorType == ActorType.BEAN || a.actorType.toString.contains(engineType.toString))
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
        actors.foreach {
          actor =>
            actor.database = tableName.split("\\.").head
            actor.baseClass = baseClass
            actor.fieldMeta = fieldMeta
            actor.init()

            val dirName = actor.actorType.toString.toLowerCase()
            val fileName = s"$baseClass${actor.actorType}.scala"
            FileUtil.saveFile(Seq[String](actor.toString), s"$projectPath/$dirName/$fileName")
        }
    }

    if(FileUtil.isExists(s"$savePath/$projectName")){
      val zip = new ZipCompress(s"$savePath/$projectName.zip", s"$savePath/$projectName")
      zip.zip()
      FileUtil.deleteFiles(s"$savePath/$projectName")
    }else{
      println(s"$savePath/$projectName donen't exists, please set table or schema for gojira.")
    }
  }

  private def printSchema(actorTypeStr: String): Unit = {
    val currActors = if (actorTypeStr == "") actors else actors.filter(_.actorType.toString.contains(actorTypeStr))
    schema.foreach {
      case (tableName, baseClass, fieldMeta) =>
        currActors.foreach {
          actor =>
            actor.database = tableName.split("\\.").head
            actor.baseClass = baseClass
            actor.fieldMeta = fieldMeta
            actor.init()
            println(actor.toString)
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
