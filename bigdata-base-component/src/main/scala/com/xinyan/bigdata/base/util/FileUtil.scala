package com.xinyan.bigdata.base.util

import java.io._
import java.util.zip.{ZipEntry, ZipOutputStream}

/**
  * Author: chentianzeng
  * Date: 2019/6/5
  * Email: tianzeng_chen@xinyan.com
  */
object FileUtil {

  def mkdir(dirPath: String): Unit = {
    val path = new File(dirPath)
    if (!path.exists()) {
      path.mkdirs()
    }
  }

  def isExists(dirPath: String): Boolean = {
    val path = new File(dirPath)
    path.exists()
  }

  /**
    * 将字符串数组的内容写入到本地文件
    *
    * @param datas    数据集合，每个元素为一行，覆写方式
    * @param filePath 文件路径
    * @param isAppend 是否追加，true 数据以追加的方式写入文件，false 数据覆写只保留新写入的数据，默认覆写
    */
  def saveFile(datas: Seq[String], filePath: String, isAppend: Option[Boolean] = Some(false)) = {
    val file = new File(filePath)
    if (!file.getParentFile.exists()) {
      file.getParentFile.mkdirs()
    }
    val writer = new PrintWriter(new FileWriter(file, isAppend.getOrElse(false)))
    try {
      datas.foreach(writer.println)
      writer.flush()
      true
    } catch {
      case e: Exception =>
        false
    } finally writer.close()
  }


  /**
    * 本地文件压缩为zip格式
    *
    * @param zipoutput zip文件输出路径
    * @param files     被压缩文件路径列表（包含文件名）
    * @return 是否成功
    */
  def zipFiles(zipoutput: String, files: List[String]): Boolean = {

    val exportFile = new File(zipoutput)
    if (!exportFile.getParentFile.exists())
      exportFile.getParentFile.mkdirs
    if (!exportFile.exists())
      exportFile.createNewFile
    //输出流
    val os: OutputStream = new FileOutputStream(exportFile)
    val zipOut: ZipOutputStream = new ZipOutputStream(os)
    try {
      for (childFileName <- files) {
        val file: File = new File(childFileName)
        val fileSize: Int = file.length().toInt
        val buf: Array[Byte] = //定义缓冲区
          if (fileSize < 1024) {
            new Array[Byte](fileSize)
          } else if (fileSize >= 1024 && fileSize < 4096) {
            new Array[Byte](1024)
          } else if (fileSize >= 4096 && fileSize < 8192) {
            new Array[Byte](4096)
          } else {
            new Array[Byte](8192)
          }
        if (file.exists()) {
          val ze: ZipEntry = new ZipEntry(file.getName)
          val input = new FileInputStream(file)
          zipOut.putNextEntry(ze)
          val bis: BufferedInputStream = new BufferedInputStream(input)
          var len = bis.read(buf)
          if (fileSize != 0) {
            while (len != -1) {
              zipOut.write(buf, 0, len)
              len = bis.read(buf)
            }
          }

          input.close()
          zipOut.closeEntry()
        }
      }
      true
    } catch {
      case e: Exception => e.printStackTrace()
        false
    } finally {
      zipOut.close()
      //关闭输出流
      os.close()
    }
  }


  /**
    * 删除文件或文件夹，慎用
    *
    * @param file 文件
    * @return
    */
  def deleteFiles(file: File): Boolean = {
    if (!file.exists()) false
    else {
      if (file.isFile) file.delete()
      else {
        for (f <- file.listFiles()) {
          deleteFiles(f)
        }
        file.delete()
      }
    }
  }


}
