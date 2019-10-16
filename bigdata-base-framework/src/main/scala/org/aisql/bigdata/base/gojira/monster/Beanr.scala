package org.aisql.bigdata.base.gojira.monster

import org.aisql.bigdata.base.gojira.enum.MonsterType
import org.aisql.bigdata.base.gojira.enum.MonsterType.MonsterType
import org.aisql.bigdata.base.gojira.model.{ClassModel, FieldMeta}
import org.aisql.bigdata.base.util.DateUtil

/**
  * Author: xiaohei
  * Date: 2019/9/15
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
class Beanr(basePackage: String, whoami: String) extends Ancestor {

  logger.info(s"${this.getClass.getSimpleName} init")

  override val monsterType: MonsterType = MonsterType.BEAN

  override protected var pkgName = s"package $basePackage.dal.bean"

  override protected var impPkgs: String =
    """
      |import java.sql.Timestamp
      |import java.sql.Date
      |import com.alibaba.fastjson.JSONObject
    """.stripMargin

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
    *
    * 设置classModel相关字段
    **/
  override def init(): Unit = {
    val fields = fieldMeta.map {
      case FieldMeta(fieldName, fieldType, fieldComment) =>
        s"""
           |  /**
           |    * $fieldComment
           |    **/
           |  var $fieldName:$fieldType = _
           |  """.stripMargin
    }.mkString("")

    val toStringStr =
      s"""
         |  override def toString = {
         |    s"${
        fieldMeta.map {
          case FieldMeta(fieldName, _, _) => "$" + fieldName
        }.mkString(",")
      }"
         |  }
    """.stripMargin

    val toJSONStringStr =
      s"""
         |  def toJSONString = {
         |    val json = new JSONObject()
         |${
        fieldMeta.map {
          case FieldMeta(fieldName, _, _) => s"    json.put('$fieldName', $fieldName)".replace("'", "\"")
        }.mkString("\n")
      }
         |    json.toJSONString
         |  }
     """.stripMargin

    classModel = initClassModel
    classModel.setImport(impPkgs)
    classModel.setAuthor(author)
    classModel.setFields(fields)
    classModel.setMethods(toStringStr)
    classModel.setMethods(toJSONStringStr)
    logger.info(s"$monsterType class model done")
  }

  private def initClassModel: ClassModel = {
    //最后行不可换行,保持 class{ 风格的代码
    val classHeader =
      s"""
         |class $baseClass$monsterType extends Serializable""".stripMargin
    new ClassModel(pkgName, classHeader)
  }


}
