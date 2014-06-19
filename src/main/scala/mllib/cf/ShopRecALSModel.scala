package mllib.cf


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

/**
 * Created by phoenix on 5/7/14.
 */
object ShopRecALSModel {

  case class Params( kryo: Boolean = false,
                     numIterations: Int = 10,
                     lambda: Double = 1.0,
                     rank: Int = 10)


  def main(args: Array[String]) {

    if(args.size < 3) {
      println("Usage: ShopRecALSModel master /input/path1 /output/file")
      System.exit(0)
    }
    val conf = new SparkConf().setMaster(args(0))
      .setAppName("ShopRecALSModel")
//      .set("spark.executor.memory","10g")
    val sc = new SparkContext(conf)

    val defaultParams = Params()
    val inputPath = args(1)
    val outputPath = args(2)
//    run(sc, defaultParams, inputPath, outputPath)

  }

  def run(sc:SparkContext, params: Params, input:String, output:String) {

    val training = sc.textFile(input).map { line =>
      val fields = line.split("\001")
      (fields(0).toInt, Rating(fields(1).toInt, fields(2).toInt, fields(3).toDouble))
    }.cache()

    val numRatings = training.count()
    val numGroups = training.map(_._1).distinct().count()
    val numUsers = training.map(_._2.user).distinct().count()
    val numShops = training.map(_._2.product).distinct().count()

    println(s"Got $numRatings ratings from $numUsers users on $numShops shops in $numGroups cms groups.")

    println("first record:  " + training.first())

    val models = new scala.collection.mutable.HashMap[Int, MatrixFactorizationModel]
    val cmsGroupsTrain = training.map(_._1).distinct().collect()
    for(g <- cmsGroupsTrain) {
      models.put(g,ALS.trainImplicit(training.filter(_._1 == g).map(_._2).repartition(100),params.rank, params.numIterations, params.lambda,1.0))
    }

    for(k <- models.keys) {
      val model = models.get(k).get
      model.userFeatures.saveAsObjectFile(output+"/alsmodels/model-uf"+k)
      model.productFeatures.saveAsObjectFile(output+"/alsmodels/model-pf"+k)
    }

    sc.makeRDD(models.keySet.toArray).saveAsTextFile(output+"/cmsgroups")

    println(s"cmsGroupTrain: $cmsGroupsTrain")
    println("models count:" + models.size)

    training.unpersist(blocking = false)

//    val testdata = sc.textFile(input(1)).map { line =>
//      val fields = line.split(("\001"))
//      (fields(0).toInt, (fields(1).toInt,fields(2).toInt))
//    }
//    println("first data for prediction: " + testdata.first())
//
////    var result = null.asInstanceOf[RDD[Rating]]
//    val cmsGroups = testdata.map(_._1).distinct().collect()
//    for(g <- cmsGroups) {
//      if (models.contains(g)){
//        val tmp = models.get(g).get.predict(testdata.filter(_._1 == g).map(_._2).repartition(500))
//        if (tmp != null) {
//          tmp.cache()
//          val tmpstr = tmp.map(x => x.user.toString + "," + x.product.toString + "," + x.rating.toString)
//          tmpstr.saveAsTextFile(output+"-"+g)
//          tmp.unpersist(blocking=false)
//        }
//      }
//    }

//    val top30 = result.top(30)(new OrdRating)

//    val top30 = sc.makeRDD(result.top(30)(new OrdRating))
//    top30.saveAsTextFile(output)

    sc.stop()
  }


}


