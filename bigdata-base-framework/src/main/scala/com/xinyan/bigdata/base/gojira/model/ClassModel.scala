package com.xinyan.bigdata.base.gojira.model

import scala.collection.mutable.ArrayBuffer

/**
  * Author: xiaohei
  * Date: 2019/9/15
  * Email: xiaohei.info@gmail.com
  * Host: xiaohei.info
  */
class ClassModel(private val pkgStr: String,
                 private val clsStr: String
                ) extends Serializable {
  private val clsResultBuffer = ArrayBuffer[String]()

  private val CLASS_OPEN: String = "{"
  private val CLASS_CLOSE: String = "}"

  private var impStr: String = ""
  private var authorStr: String = ""

  private val fieldsStr: StringBuilder = new StringBuilder
  private val methodsStr: StringBuilder = new StringBuilder

  def setImport(imp: String) = {
    impStr = imp
  }

  def setAuthor(author: String) = {
    authorStr = author
  }

  def setFields(fields: String) = {
    fieldsStr.append(fields)
  }

  def setMethods(methods: String) = {
    methodsStr.append(methods)
  }

  override def toString: String = {
    clsResultBuffer.append(pkgStr)
    clsResultBuffer.append("\n")
    clsResultBuffer.append(impStr)
    clsResultBuffer.append(authorStr)
    //与className在同一行,保持 class{ 的风格
    clsResultBuffer.append(s"$clsStr $CLASS_OPEN")
    clsResultBuffer.append("\n")
    clsResultBuffer.append(fieldsStr.toString())
    clsResultBuffer.append(methodsStr.toString())
    clsResultBuffer.append("\n")
    clsResultBuffer.append(CLASS_CLOSE)
    clsResultBuffer.mkString("")
  }
}
