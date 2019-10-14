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
  private val classResultBuffer = ArrayBuffer[String]()

  private val CLASS_OPEN: String = "{"
  private val CLASS_CLOSE: String = "}"

  private var impStr: String = ""
  private var authorStr: String = ""

  private val fieldsBuffer: StringBuilder = new StringBuilder
  private val methodsBuffer: StringBuilder = new StringBuilder

  def setImport(imp: String) = {
    impStr = imp
  }

  def setAuthor(author: String) = {
    authorStr = author
  }

  def setFields(fields: String) = {
    fieldsBuffer.append(fields)
  }

  def setMethods(methods: String) = {
    methodsBuffer.append(methods)
  }

  override def toString: String = {
    classResultBuffer.clear()
    classResultBuffer.append(pkgStr)
    classResultBuffer.append("\n")
    classResultBuffer.append(impStr)
    classResultBuffer.append(authorStr)
    //与className在同一行,保持 class{ 的风格
    classResultBuffer.append(s"$clsStr $CLASS_OPEN")
    classResultBuffer.append("\n")
    classResultBuffer.append(fieldsBuffer.toString())
    classResultBuffer.append(methodsBuffer.toString())
    classResultBuffer.append("\n")
    classResultBuffer.append(CLASS_CLOSE)
    classResultBuffer.mkString("")
  }
}
