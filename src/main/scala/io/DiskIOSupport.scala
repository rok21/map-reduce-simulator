package io

import java.io.{File, PrintWriter}

trait DiskIOSupport {
  protected def readFile(fileName: String): List[String] = {
    val file = new File(fileName)
    val bufferedSource =scala.io.Source.fromFile(file)
    val fileContent = bufferedSource.getLines().toList
    bufferedSource.close()
    fileContent
  }

  protected def writeFile(fileName: String, contents: Seq[String]) = {
    val file = new File(fileName)
    file.getParentFile().mkdirs()
    file.createNewFile()
    val pw = new PrintWriter(file)
    contents.foreach(pw.println)
    pw.close
  }
}
