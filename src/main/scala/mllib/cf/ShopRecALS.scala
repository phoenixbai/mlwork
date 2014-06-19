package mllib.cf


import com.esotericsoftware.kryo.Kryo
import org.apache.log4j.{Level, Logger}
import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.{KryoSerializer, KryoRegistrator}
import org.junit.Ignore

/**
 * Created by phoenix on 5/7/14.
 */
@deprecated
object ShopRecALS {

  class ALSRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
      kryo.register(classOf[Rating])
    }
  }

  case class Params(
                     input: String = null,
                     kryo: Boolean = false,
                     numIterations: Int = 20,
                     lambda: Double = 1.0,
                     rank: Int = 10,
                     master: String = "yarn-standalone")


  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("ShopRecALS") {
      head("ShopRecALS: 从微群发现同好店铺测试")
      opt[Int]("rank")
        .text(s"rank, default: ${defaultParams.rank}}")
        .action((x, c) => c.copy(rank = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("lambda")
        .text(s"lambda (smoothing constant), default: ${defaultParams.lambda}")
        .action((x, c) => c.copy(lambda = x))
      opt[Unit]("kryo")
        .text(s"use Kryo serialization")
        .action((_, c) => c.copy(kryo = true))
      arg[String]("<input>")
        .required()
        .text("input paths to a user shop browsing dataset")
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }
  }

  def run(params: Params) {
//    val conf = new SparkConf().setAppName(s"ShopRecALS with $params")
    println(params.master)
    val conf = new SparkConf().setMaster(params.master)
      .setAppName("ShopRecALS test")

    if (params.kryo) {
      conf.set("spark.serializer", classOf[KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[ALSRegistrator].getName)
        .set("spark.kryoserializer.buffer.mb", "8")
    }
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val ratings = sc.textFile(params.input).map { line =>
       val fields = line.split("\001")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }.cache()

    val numRatings = ratings.count()
    val numUsers = ratings.map(_.user).distinct().count()
    val numShops = ratings.map(_.product).distinct().count()

    println(s"Got $numRatings ratings from $numUsers users on $numShops movies.")

    val training = ratings.cache()
    val numTraining = training.count()
//    val numTest = test.count()
    println(s"Training: $numTraining")

    ratings.unpersist(blocking = false)

    val model = ALS.trainImplicit(training, params.rank, params.numIterations, params.lambda,1,1.0)
    //35982869 sammisky
    val shoplist = ratings.map(x => (124245130, x.product)).distinct()
    val result = model.predict(shoplist)


    val top20 = result.top(30)(new OrdRating)

    for (i <- top20)
      println("shopId:" + i.product + " score: " + i.rating)

//    top20.saveAsTextFile("/home/phoenix/dataset/sparktest/shopRecAlsOut.txt")

//    println(model)
//    println(model.predict(124245130,677608399))
//    println(model.predict(39491912,829338140))

//    val rmse = computeRmse(model, test, numTest)

//    println(s"Test RMSE = $rmse.")

    sc.stop()
  }

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long) = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }

}
