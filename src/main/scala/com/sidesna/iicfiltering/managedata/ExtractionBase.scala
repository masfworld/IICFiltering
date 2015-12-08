package com.sidesna.iicfiltering.managedata

import org.apache.spark.sql.{SQLContext, DataFrame}

/**
 * Created by Miguel A. Sotomayor
 * Date: 18/10/15
 *
 *
 */
trait ExtractionBase {
  def getCustomers(sqlContext: SQLContext): DataFrame
  def getProducts(sqlContext: SQLContext): DataFrame
  def getPurchases(sqlContext: SQLContext): DataFrame
}
