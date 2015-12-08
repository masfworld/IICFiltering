package com.sidesna.iicfiltering

import breeze.linalg.Vector
import com.sidesna.iicfiltering.helpers.MLCalculation
import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Created by Miguel A. Sotomayor 
  * Date: 29/10/15
  *
  */
class IICFilter(sc: SparkContext, sqlContext : HiveContext, customers: DataFrame, products : DataFrame, purchases: DataFrame) {

  val topN_Elements = 5

  private[this] val logger = Logger.getLogger(getClass.getName)

  def createRecommendations() : DataFrame = {
    import sqlContext.implicits._

    //Join products and purchases to get products purchased by customer
    val prodCustoJoin = products
      .join(purchases, purchases("product_id") === products("pro_id"))
      .select($"user_id", $"product_id")

    //logger.trace(s"ProdCustoJoin schema...: ${prodCustoJoin.printSchema()}")

    //Inverting ordering producing key-value.
    val custoProduct = prodCustoJoin
      .map(x => (x.getAs[Long]("user_id"), x.getAs[Long]("product_id")))

    //It's persisted in memory because this DataFrame will be used more than once
    custoProduct.persist()

    //First, we have to get a product pair list which have been purchased together
    val productTuple = getItemsPurchasedTogether(custoProduct)
    logger.info("Calculating products purchased together....")
    logger.trace(s"Products tuple purchased together (count): ${productTuple.count}")
    //logger.trace(s"Products tuple purchased together (count): ${productTuple.take(20).foreach(println)}")

    val productVector = getProductsVector(custoProduct)

    val itemItemSimilarity = getRDDSimilarity(productVector, productTuple)

    getTopNItems(itemItemSimilarity)
    /*logger.info("Calculating similarity....")
    logger.trace(s"Similarity RDD length: ${itemSimilarityRDD.count}")
    logger.trace(s"Sample (20) similarity: ${itemSimilarityRDD.take(20).foreach(println)}")*/

  }

  /*
    This method returns a RDD with product pairs which are purchased together
    First, the customers are grouped
    Second, get the product tuple for each customer
    At last, a distinct function is applied to get the product tuple unique
  */
  def getItemsPurchasedTogether(custoProduct: RDD[(Long, Long)]): RDD[(Long, Long)] = {

    //The result is a product list for each customer
    //Filtering is because customers with 1 item purchased is not useful
    val custoProductGrouped = custoProduct
      .groupByKey
      .filter(_._2.toSeq.length > 1)

    //logger.trace(s"custoProductGrouped: ${custoProductGrouped.take(20).foreach(println)}")

    //Return distinct products tuple which have been purchased by the same customer
    custoProductGrouped.flatMap(x => {
      for {
        item1 <- x._2
        item2 <- x._2
        if item1 != item2
      } yield (math.min(item1, item2), math.max(item1, item2))

    }).distinct

  }

  /*
  TODO: Allow vectors where number of customer greater than integer scope

  This method returns a vector per product where each vector has M dimensions, being M the number of customers
  By default each position of the vector is zero, set value to 1 if the customer has purchased the product.
  * */
  def getProductsVector(custoProductRDD: RDD[(Long, Long)]): RDD[(Long, Vector[Double])] = {

    //Get unique index by customer
    val custWithIndex = customers.map(_.getAs[Long]("usu_id")).zipWithIndex //(key = customer, value = IndexCustomer)
    val numCustomers = custWithIndex.count.toInt
    logger.trace(s"Number of customers: $numCustomers")

    //Join between products-customer with customer with index
    //custoProductRDDJoinIndex => (key = customer, value = (product, IndexCustomer))
    val custoProductRDDJoinIndex = custoProductRDD.join(custWithIndex)

    //Exchange key-value to get the key as product and customer-index
    //The result of the map => (key = product, value = (customer, IndexCustomer))
    //Group by key, that is, by product => (product, list((customer, IndexCustomer))
    val productCusto = custoProductRDDJoinIndex.map(x => (x._2._1, (x._1, x._2._2))).groupByKey

    productCusto.map(x => {
      val myVector = new Array[Double](numCustomers)

      val iterator = x._2.iterator
      iterator.foreach(item => {
        myVector(item._2.toInt) = 1.0d
      })
      //Return product, Vector
      (x._1, Vector(myVector))
    })

  }

  /*
  This method return a DataFrame mapped from ItemItemSimilarity
  * */
  def getRDDSimilarity(productVector: RDD[(Long, Vector[Double])], productTuples: RDD[(Long, Long)]) : DataFrame ={

    import sqlContext.implicits._

    val productVectorBC = sc.broadcast(productVector.collectAsMap())

    productTuples.map( x => {
      val vector1 = productVectorBC.value.filter(y => y._1 == x._1.toLong).values.head
      val vector2 = productVectorBC.value.filter(y => y._1 == x._2.toLong).values.head
      (x, MLCalculation.cosineSimilarity(vector1, vector2))
    }).map(i => ItemItemSimilarity(i._1._1, i._1._2, i._2)).toDF()

  }


  /*
  This method takes itemItemSimilarity dataframe which has a pair items and its similarity
  It performs the following operations:
  1.- group by item1 and select the best top N similarity items
  2.- group by item2 and select the best top N similarity items and it exchanges the item1 and item2 values.
      In this way, in
  3.- Union dataframes resulted from point 1 and 2, where the first item (item grouped) is the itemMain and the other
      is the item related
  4.- Group by itemMain and return the best top 5 items related
  * */
  def getTopNItems(itemItemSimilarity : DataFrame) : DataFrame = {
    import sqlContext.implicits._

    //Point 1
    val overItem1 = Window
      .partitionBy("item1")
      .orderBy($"similarity".desc)
    val rank1 = rowNumber.over(overItem1)
    val ranked1 = itemItemSimilarity.withColumn("rank", rank1)
    val ranked1Filter = ranked1
      .where(ranked1("rank") <= topN_Elements)
      .select($"item1" as "itemMain", $"item2" as "itemRelated", $"similarity")

    //Point 2
    val overItem2 = Window
      .partitionBy("item2")
      .orderBy($"similarity".desc)
    val rank2 = rowNumber.over(overItem2)
    val ranked2 = itemItemSimilarity.withColumn("rank", rank2)
    val ranked2Filter = ranked2
      .where(ranked2("rank") <= topN_Elements)
      .select($"item2" as "itemMain", $"item1" as "itemRelated", $"similarity")

    //Point 3
    val rankedFilterUnion = ranked1Filter.unionAll(ranked2Filter)

    //Point 4
    val overItem = Window
      .partitionBy("itemMain")
      .orderBy($"similarity".desc)
    val rank = rowNumber().over(overItem)
    val ranked = rankedFilterUnion.withColumn("rank", rank)
    ranked.where(ranked("rank") <= topN_Elements).select($"itemMain", $"itemRelated", $"similarity")

  }
}
