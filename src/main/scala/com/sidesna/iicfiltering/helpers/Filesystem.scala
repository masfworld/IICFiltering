package com.sidesna.iicfiltering.helpers

import java.io.InputStream

/**
 * Created by Miguel A. Sotomayor 
 * Date: 18/10/15
 *
 *
 */
trait Filesystem {
  def getFile(path: String): String
  def getFile(in: InputStream): String
}
