package com.sidesna.iicfiltering.managedata.mysql

import org.apache.spark.sql.{SaveMode, DataFrame}

/**
  * Created by Miguel A. Sotomayor 
  * Date: 6/12/15
  *
  *
  */
object MysqlExport extends Mysql {

  def exportDataframe(df : DataFrame, table: String) = {

    val prop = getConfig

    val properties = new java.util.Properties

    df.write
      .mode(SaveMode.Overwrite)
      .jdbc(s"jdbc:mysql://${prop.hostname}:${prop.port}/${prop.database}?user=${prop.username}&password=${prop.password}", table, properties)

  }
}
