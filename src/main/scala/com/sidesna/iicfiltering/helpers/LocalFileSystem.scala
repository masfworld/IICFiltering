package com.sidesna.iicfiltering.helpers

import java.io.InputStream

/**
 * Created by Miguel A. Sotomayor 
 * Date: 18/10/15
 *
 *
 */
object LocalFileSystem extends Filesystem{
  override def getFile(path: String): String = scala.io.Source.fromFile(path).mkString

  override def getFile(in: InputStream): String = scala.io.Source.fromInputStream(in).mkString
}
