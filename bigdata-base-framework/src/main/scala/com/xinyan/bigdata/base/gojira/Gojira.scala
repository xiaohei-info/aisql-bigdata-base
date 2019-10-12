package com.xinyan.bigdata.base.gojira

import com.xinyan.bigdata.base.gojira.actor.sparkimpl.{SparkDaor, SparkServicr}
import com.xinyan.bigdata.base.gojira.actor.{Ancestor, Beanr}
import com.xinyan.bigdata.base.util.{FileUtil, HiveUtil, StringUtil}
import org.apache.spark.sql.SparkSession

/**
  * Author: xiaohei
  * Date: 2019/9/11
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
class Gojira(savePath: String, projectName: String,
             projectPkgName: String, whoami: String, tableNames: Seq[String],
             tableSchema: Seq[(String, String, Seq[(String, String, String)])] = Seq.empty) {

  private val actors: Seq[Ancestor] = Seq[Ancestor](
    new Beanr(projectPkgName, whoami),
    new SparkDaor(projectPkgName, whoami),
    new SparkServicr(projectPkgName, whoami)
  )

  private val schema: Seq[(String, String, Seq[(String, String, String)])] = {
    //    if (tableSchema.isEmpty) {
    //      tableNames.map {
    //        tableName =>
    //          val baseClass: String = StringUtil.under2camel(tableName.split("\\.").last)
    //          val fieldMeta: Seq[(String, String, String)] = HiveUtil.getScheme(spark, tableName)
    //          (tableName, baseClass, fieldMeta)
    //      }
    //    } else {
    //      tableSchema
    //    }
    tableSchema
  }

  private val projectPath = savePath + "/" + projectName
  private val beanPath = projectPath + "/bean"
  private val daoPath = projectPath + "/dao"
  private val servicePath = projectPath + "/service"

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
    if (!preMkdir) return
    schema.foreach {
      case (tableName, baseClass, fieldMeta) =>
        actors.foreach {
          actor =>
            actor.database = tableName.split("\\.").head
            actor.baseClass = baseClass
            actor.fieldMeta = fieldMeta
            actor.init()
            FileUtil.saveFile(Seq[String](actor.toString), beanPath + s"/$baseClass${actor.actorType}.scala")
        }
    }
  }

  private def printSchema(actorType: String): Unit = {
    val currActors = if (actorType == "") actors else actors.filter(_.actorType == actorType)
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

  private def preMkdir: Boolean = {
    if (!FileUtil.isExists(savePath)) {
      println("path does exists!")
      return false
    }

    FileUtil.mkdir(beanPath)
    FileUtil.mkdir(daoPath)
    FileUtil.mkdir(servicePath)
    true
  }

}
