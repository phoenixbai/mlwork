package mllib.clustering

import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.PairRDDFunctions
import SparkContext._

/**
 * Created by phoenix on 6/10/14.
 */
object KmeansPrice {

  val iterations = 30
  var clusterNum = 10
  def main(args:Array[String]) {
    if(args.length < 4) {
      println("Usage: KmeansPrice master input output clusterNum [filetype]")
    }
    val conf = new SparkConf().setMaster(args(0))
                              .setAppName("clustering auctions based on price!")

    val sc = new SparkContext(conf)
    val input = args(1)
    val output = args(2)
    var filetype = 1
    clusterNum = args(3).toInt
    if(args.length>4)
        filetype = args(4).toInt

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    // Delete the existing path, ignore any exceptions thrown if the path doesn't exist
    try { hdfs.delete(new org.apache.hadoop.fs.Path(output), true) } catch { case _ : Throwable => { } }

    run(sc,input, output, filetype)
    sc.stop()

  }

  def run(sc:SparkContext, input:String, output:String, filetype:Int) {

    val data =
      if(filetype==0) sc.textFile(input).map {
                        case (v) =>
                          val fields = v.split("\001")
                          new MakeupBrand(fields(0), fields(1), fields(2), fields(3).toDouble)
                      }
      else sc.sequenceFile(input, classOf[String], classOf[org.apache.hadoop.io.Text]).map {
              case (k,v) =>
                val fields = v.toString.split("\001")
                new MakeupBrand(fields(0), fields(1), fields(2), fields(3).toDouble)
            }.cache()

    println(data.first().brand+","+data.first().price+","+data.first().leafcat)

    val leafCats = data.map(_.leafcat).distinct().collect()
    println("leafCats count:" + leafCats.length)

    val predictData = leafCats.map{leaf =>
      val leafdata = data.filter( f => f.leafcat.equals(leaf))
      println("leaf:" + leaf + ", count:" + leafdata.count())
      val ldataCount = leafdata.count()

      if (ldataCount <=100 && ldataCount > 20) clusterNum = 5
      else if (ldataCount <=20 && ldataCount>5) clusterNum = 2
      else if(ldataCount<=5) clusterNum = 1

      val prices = leafdata.map(x => Vectors.dense(x.price))
      val clusters = KMeans.train(prices, clusterNum, iterations)
      val predicts = clusters.predict(prices)
      leafdata.zip(predicts)
    }.reduce(_.union(_))

    val result = predictData.repartition(1).map( v =>
      v._1.brand +"," + v._1.leafcat +"," + v._1.cat1 +"," + v._1.price+ "," + v._2)
    result.saveAsTextFile(output)
  }


}
