package com.sidesna.iicfiltering.managedata.mysql

import com.sidesna.iicfiltering.managedata.ExtractionBase
import org.apache.spark.sql.{SQLContext, DataFrame}
import com.sidesna.iicfiltering.helpers._
import play.api.libs.json.Json


/**
 * Created by Miguel A. Sotomayor 
 * Date: 18/10/15
 *
 *
 *
 */
class MysqlImport() extends ExtractionBase with Mysql{

  import org.apache.spark.sql.functions._

  val toInt    = udf[Int, String]( _.toInt)
  val toLong   = udf[Long, String]( _.toLong)
  val toDouble = udf[Double, String]( _.toDouble)
  val toHour   = udf((t: String) => "%04d".format(t.toInt).take(2).toInt )


  override def getCustomers(sqlContext: SQLContext): DataFrame = {

    val prop = getConfig

    val query = "SELECT usu_id FROM usuario"

    val options = collection.immutable.Map[String, String](
      "driver" -> prop.driver,
      "url" -> s"jdbc:mysql://${prop.hostname}:${prop.port}/${prop.database}?user=${prop.username}&password=${prop.password}",
      "dbtable" -> s"($query) as usuario")

    sqlContext.read.format("jdbc").options(options).load()
  }

  override def getProducts(sqlContext: SQLContext): DataFrame = {
    val prop = getConfig

    val query = "SELECT pro_id FROM producto"

    val options = collection.immutable.Map[String, String](
      "driver" -> prop.driver,
      "url" -> s"jdbc:mysql://${prop.hostname}:${prop.port}/${prop.database}?user=${prop.username}&password=${prop.password}",
      "dbtable" -> s"($query) as productos")

    //sqlContext.load("jdbc", options)
    sqlContext.read.format("jdbc").options(options).load()
  }

  override def getPurchases(sqlContext: SQLContext): DataFrame = {
    val prop = getConfig

    val query = "SELECT ped_usu_id as user_id, pedxpro_pro_id as product_id " +
      "FROM pedido ped inner join pedxpro on (ped.ped_id = pedxpro.pedxpro_ped_id) " +
      "WHERE pedxpro_pro_id > '' " +
      "GROUP BY ped_usu_id, pedxpro_pro_id"

    val options = collection.immutable.Map[String, String](
      "driver" -> prop.driver,
      "url" -> s"jdbc:mysql://${prop.hostname}:${prop.port}/${prop.database}?user=${prop.username}&password=${prop.password}",
      "dbtable" -> s"($query) as pedidos")

    //val df = sqlContext.load("jdbc", options)
    val df = sqlContext.read.format("jdbc").options(options).load()
    df.withColumn("product_id", toLong(df("product_id")))
  }
}
