package mllib.clustering

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by phoenix on 6/9/14.
 */
object KmeansTest {

  def main(args:Array[String]) {

    if(args.length < 3) {
      println("Usage: KmeansTest master input output")
      System.exit(0)
    }

    val conf = new SparkConf().setMaster(args(0))
      .setAppName("KmeansTest")

    val sc =  new SparkContext(conf)
    // Load and parse the data
    val data = sc.textFile(args(1)).map {
      case (v) =>
        val fields = v.split("\001")
        new MakeupBrand(fields(0), fields(1), fields(2), fields(3).toDouble)
    }
//    val data = sc.sequenceFile(args(1), classOf[String], classOf[String]).map {
//      case (k,v) =>
//        val fields = v.split("\001")
//        new MakeupBrand(fields(0), fields(1), fields(2), fields(3).toDouble)
//    }
    val prices = data.map(x => Vectors.dense(x.price))

    // Cluster the data into two classes using KMeans
    val numClusters = 10
    val numIterations = 40
    val clusters = KMeans.train(prices, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
//    val WSSSE = clusters.computeCost(parsedData)
//    println("Within Set Sum of Squared Errors = " + WSSSE)
    val predictions = clusters.predict(prices)
    val result = data.zip(predictions).map( v =>
            v._1.brand +"," + v._1.leafcat +"," + v._1.cat1 +"," + v._1.price+ "," + v._2)
    result.saveAsTextFile(args(2))

    sc.stop()
  }

}

