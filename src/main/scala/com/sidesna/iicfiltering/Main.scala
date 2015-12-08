package com.sidesna.iicfiltering

import com.sidesna.iicfiltering.managedata.mysql.MysqlExport
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame

/**
 * Created by Miguel A. Sotomayor 
 * Date: 18/09/15
 *
 *
 */
object Main {

  def main(args: Array[String]) {

    //val trainingData = 0.8

    //val conf = new SparkConf().setAppName("Spark Recommendations").setMaster(args(0))
    val sc = new SparkContext()

    //val sqlContext = new SQLContext(sc)
    val sqlContext = new HiveContext(sc)

    val databaseAdapter = new com.sidesna.iicfiltering.managedata.mysql.MysqlImport()

    val customers = databaseAdapter.getCustomers(sqlContext)
    //customers.printSchema()

    val products = databaseAdapter.getProducts(sqlContext)
    //products.printSchema()

    val purchases = databaseAdapter.getPurchases(sqlContext)
    //purchases.printSchema()

    //val miniproducts = products.filter("pro_id between 50 and 56").toDF()
    //miniproducts.toDF.registerTempTable("miniproducts")

    val purchasesSplitted = splitPurchasesTrainingTest(customers, purchases)
    val purchasesTraining = purchasesSplitted._1
    val purchasesTest = purchasesSplitted._2

    val filteringTraining = new IICFilter(sc, sqlContext, customers, products, purchasesTraining)

    val itemSimilarity = filteringTraining.createRecommendations()

    MysqlExport.exportDataframe(itemSimilarity, "recommendations")

    //val filteringTest = new IICFilterEvaluate(itemSimilarity ,purchasesTest)

    //filteringTest.calculateError(sqlContext)


    sc.stop()
  }


  /*
  This method split the customer dataframe in training and test.
  It returns two dataframe. The first is the purchases dataframe filtered with the customer training dataframe,
  the another one is the purchases with the customer test dataframe.
  * */
  def splitPurchasesTrainingTest(customers: DataFrame, purchases: DataFrame) : (DataFrame, DataFrame) = {
    val splits = customers.randomSplit(Array(1.0, 0.0))

    val purchasesTraining = purchases.join(splits(0), purchases("user_id") === splits(0)("usu_id")).select("user_id", "product_id")
    val purchasesTest = purchases.join(splits(1), purchases("user_id") === splits(1)("usu_id")).select("user_id", "product_id")

    (purchasesTraining, purchasesTest)
  }

}
