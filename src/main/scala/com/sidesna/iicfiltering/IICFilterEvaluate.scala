package com.sidesna.iicfiltering

import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.functions._

/**
  * Created by Miguel A. Sotomayor 
  * Date: 30/11/15
  *
  *
  */
class IICFilterEvaluate(itemSimilarity : DataFrame, purchasesTest : DataFrame) {

   def calculateError(sqlContext: SQLContext) = {

     import sqlContext.implicits._
     //Cache purchasesTest because it's going to use more than once
     purchasesTest.persist()

     //Calculate the number of customers inside of test dataset
     val numberOfCustomers = purchasesTest
       .groupBy("user_id")
       .agg(count("product_id") as "counter")
       .filter($"counter" > 1)
       .distinct()
       .count()

     val customersWithRelations = getCustomersRelated(itemSimilarity, purchasesTest)

     println(s"Number of customers initially: $numberOfCustomers")
     println(s"Number of customers with Similarity: $customersWithRelations")
     println(s"Accuracy: ${BigDecimal((customersWithRelations.toDouble / numberOfCustomers.toDouble) * 100.0).setScale(2, BigDecimal.RoundingMode.CEILING)} %")

   }

  /*
  This method has the following steps:
  1.- Group itemsimilarity for itemMain. ItemMain contains all items which can be related with any others.
  2.- Join the rdd resulted from step 1 with purchases (subset test).
      In this way we have a RDD with the (customer, itemMain and itemRelated to itemMain)
  3.- Afterwards, join itemMain to itemRelated
  4.- At last, to do an intersect between items of the same customer. In this way we know if there are
  * */
  def getCustomersRelated(itemSimilarity : DataFrame, purchasesTest : DataFrame) : Long = {
    //Group by itemMain
    val itemSimilarityGroupedByMain = itemSimilarity
      .map(x => (x.getAs[Long]("itemMain"), x.getAs[Long]("itemRelated")))
      .groupByKey()

    //Join purchases with itemSimilarity grouped by itemMain
    //The result is (product_id, (customer_id, (List products related to product_id)))
    val purchasesItemSimilarity = purchasesTest
      .map(x => (x.getAs[Long]("product_id"), x.getAs[Long]("user_id")))
      .join(itemSimilarityGroupedByMain)

    val purchasesItemSimilarityUniq = purchasesItemSimilarity.combineByKey(
      (init:(Long, Iterable[Long])) => (Seq(init._1), init._2.toSeq),
      (acc: (Seq[Long], Seq[Long]), s: (Long, Iterable[Long])) => {(acc._1 :+ s._1, acc._2 ++ s._2.toSeq)},
      (acc1: (Seq[Long], Seq[Long]), acc2: (Seq[Long], Seq[Long])) => (acc1._1 ++ acc2._1.toSeq, acc1._2 ++ acc2._2.toSeq)
    )

    purchasesItemSimilarityUniq
      .mapValues(x => x._1.intersect(x._2).nonEmpty)
      .filter(_._2)
      .count()


    /*val purchasesItemSimilarityUniq = purchasesItemSimilarity.map(x => {
      //Add itemMain to seq of itemsRelated
      val arrayWithProductMain = x._2._2.toSeq :+ x._1
      (x._2._1, arrayWithProductMain)
    })

    purchasesItemSimilarityUniq
      .reduceByKey((a, b) => a.intersect(b))
      .filter(x => x._2.nonEmpty).count()
*/
  }

}
