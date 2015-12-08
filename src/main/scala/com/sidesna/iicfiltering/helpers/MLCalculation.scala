package com.sidesna.iicfiltering.helpers

import breeze.linalg.{sum, Vector}

/**
  * Created by Miguel A. Sotomayor 
  * Date: 29/11/15
  *
  *
  */
object MLCalculation {

  /*
    This method calculates cosine similarity
    * */
  def cosineSimilarity(vector1: Vector[Double], vector2: Vector[Double]): Double = {
    val dotProduct = vector1 dot vector2

    val d1 = sum(vector1.map(Math.pow(_, 2)))
    val d2 = sum(vector2.map(Math.pow(_, 2)))

    if (d1 <= 0.0 || d2 <= 0.0){
      0.0
    }
    else{
      dotProduct.asInstanceOf[Double] / (Math.sqrt(d1) * Math.sqrt(d2))
    }
  }


}
