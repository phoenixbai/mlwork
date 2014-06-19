package mllib.cf

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}

/**
 * Created by phoenix on 5/14/14.
 */
object ShopRecALSPredict {
  def main(args: Array[String]) {

//    if(args.size < 3) {
//      println("Usage: ShopRecALSPredict master /model/path /testdata/path /output/path")
//      System.exit(0)
//    }
    val conf = new SparkConf().setMaster(args(0))
      .setAppName("ShopRecALSPredict ")
//      .set("spark.executor.memory","10g")
    val sc = new SparkContext(conf)

    val modelInput = args(1)
    val testInput = args(2)
    val output = args(3)

//    run(sc, modelInput, testInput, output)

  }

//  def run(sc:SparkContext, minput: String, tinput:String, output:String) {
//
//    val testdata = sc.textFile(tinput).map { line =>
//      val fields = line.split(("\001"))
//      (fields(0).toInt, (fields(1).toInt,fields(2).toInt))
//    }
//    println("first data for prediction: " + testdata.first())
//
//    val rank = 10
//    val cmsgroups = sc.textFile(minput+"/cmsgroups")
//    println(s"cmsgroups: $cmsgroups")
//
////    val models = new scala.collection.mutable.HashMap[Int, MatrixFactorizationModel]
//
//    for(g <- cmsgroups.toArray()) {
//      println(minput+"/alsmodels/model-uf"+g)
//      val userFeatures = sc.objectFile[(Int, Array[Double])](minput+"/alsmodels/model-uf"+g)
//      val productFeatures = sc.objectFile[(Int, Array[Double])](minput+"/alsmodels/model-pf"+g)
//      val model = new MatrixFactorizationModel(rank, userFeatures, productFeatures)
//
//      val gi = g.toInt
//
//      //using model to predict rating
//      val tmp = model.predict(testdata.filter(_._1 == gi).map(_._2).repartition(500))
//      if (tmp != null) {
//        tmp.cache()
//        val tmpstr = tmp.map(x => x.user.toString + "," + x.product.toString + "," + x.rating.toString)
//        tmpstr.saveAsTextFile(output+"-"+gi)
//        tmp.unpersist(blocking=false)
//      }
//
//    }
//
//    sc.stop()
//  }

//    val training = sc.textFile(input).map { line =>
//      val fields = line.split("\001")
//      (fields(0).toInt, Rating(fields(1).toInt, fields(2).toInt, fields(3).toDouble))
//    }.cache()
}
