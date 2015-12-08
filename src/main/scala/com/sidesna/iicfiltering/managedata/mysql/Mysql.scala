package com.sidesna.iicfiltering.managedata.mysql

import com.sidesna.iicfiltering.helpers.LocalFileSystem
import play.api.libs.json.Json

/**
  * Created by Miguel A. Sotomayor 
  * Date: 6/12/15
  *
  *
  */
trait Mysql {

  def getConfig: Properties = {

    val in = getClass.getResourceAsStream("/mysql.config")
    val contentConfig = LocalFileSystem.getFile(in)
    val json = Json.parse(contentConfig)

    new Properties(
      (json \ "driver").as[String],
      (json \ "username").as[String],
      (json \ "password").as[String],
      (json \ "hostname").as[String],
      (json \ "port").as[String],
      (json \ "database").as[String]
    )
  }

}
