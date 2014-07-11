package com.taobao.guang

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.recommendation.{Rating, ALS}
import SparkContext._

/**
 * Created by phoenix on 7/3/14.
 */
object LikeRelikeAls {

//  val rank = 10
//  val iternum = 20
//  val lambda = 1.0
//
//  def main(args:Array[String]) {
//    if(args.length<4) {
//      println("Usage: LikeRelikeAls input output parnum")
//      System.exit(0)
//    }
//    run(args)
//  }
//  def run(args:Array[String]) {
//    val input = args(0)
//    val output = args(1)
//    val parnum = args(2)
//
//    val conf = new SparkConf().setAppName("爱逛街之喜欢了又喜欢")
//    val sc = new SparkContext(conf)
//
//    val inputdata = sc.sequenceFile(input, classOf[String], classOf[org.apache.hadoop.io.Text])
//    val data = inputdata.map { line =>
//      val fields = line.toString().split("\001")
//      (fields(0),fields(1), fields(2), fields(3))
//    }
//    val training = data.map(x => new Rating(x._1,x._2, x._4))  //userId, auctionId, score
//
//    val model = ALS.train(training,rank, iternum, lambda)
////    ALS.trainImplicit(training.filter(_._1 == g).map(_._2).repartition(100),rank, iternum, lambda,1.0)
//   val productMatrix = model.productFeatures
//
//
//    val predict = model.predict()

//  }
}
