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
class Beanr(basePackage: String, whoami: String) extends Ancestor(whoami) {

  override val monsterType: MonsterType = MonsterType.BEAN

  override val rootClass: String = ""

  override val implPkg: String = ""

  override protected var beanClassName: String = _

  override protected var classHeader: String = _

  override protected var classModel: ClassModel = _

  override protected var pkgName = s"package $basePackage.dal.bean"

  override protected var impPkgs: String = _

  /**
    * 类初始化设置
    *
    * 设置classModel相关字段
    **/
  override def init(): Unit = {
    beanClassName = s"$baseClass${MonsterType.BEAN}"

    impPkgs =
      """
        |import java.sql.Timestamp
        |import java.sql.Date
        |import com.alibaba.fastjson.JSONObject
        |import scala.beans.BeanProperty
      """.stripMargin

    //最后行不可换行,保持 class{ 风格的代码
    classHeader =
      s"""
         |class $baseClass$monsterType extends Serializable""".stripMargin

    classModel = new ClassModel(pkgName, classHeader)

    val fields = fieldMeta.map {
      case FieldMeta(fieldName, fieldType, fieldComment) =>
        s"""
           |  /**
           |    * $fieldComment
           |    **/
           |  @BeanProperty
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

    classModel.setImport(impPkgs)
    classModel.setAuthor(author)
    classModel.setFields(fields)
    classModel.setMethods(toStringStr)
    classModel.setMethods(toJSONStringStr)
    logger.info(s"$monsterType class model done")
  }

}
