package business

import org.apache.spark.{SparkContext, SparkConf}
import SparkContext._
import org.junit.Ignore

/**
 * Created by phoenix on 5/21/14.
 */

object YzbbResearch {


  def main(args:Array[String]) {
//    if(args.size < 4) {
//      println("Usage: YzbbResearch master /input/path /output/path")
//      System.exit(0)
//    }
//
//    val conf = new SparkConf().setMaster(args(0))
//                  .setAppName("优质宝贝评估")
//    val sc = new SparkContext(conf)
//
//    val input = args(1)
//    val output = args(2)



  }

//  def run(sc:SparkContext, input:Array[String], output:String) {
//    val browseData = sc.sequenceFile(input(0), classOf[String], classOf[String]).map {
//      case (k, v) =>
//        val fields = v.split("\001")
//      ((fields(0),fields(1)), new BrowseInfo(fields(0), fields(1), fields(3), fields(4), fields(5), fields(13)))
//      }.filter(!_._2.user.isEmpty).filter(_._2.spm.contains("a217j.7271397."))
//
//    val gmvData = sc.sequenceFile[String,String](input(1)).map{
//      case (k,v) =>
//        val fields = v.split("\001")
//        ((fields(11), fields(13)), new GmvInfo(fields(11),fields(13),fields(4), fields(12),fields(2)))
//      }.filter(_._2.gmv.equalsIgnoreCase("gmv"))
//
//    val behaviorData = browseData.leftOuterJoin(gmvData).filter {
//      case ((k1,k2), (v1,opt)) => {
//        opt match {
//          case None => return true
//          case Some(v2) => {
//            v1.viewtime.compareTo(v2.gmvtime) < 0
//          }
//        }
//      }
//    }




//  }
}

class BrowseInfo(val user:String, val auction:String, val category:String, val seller:String, val viewtime:String, val spm:String)  extends Serializable
class GmvInfo(val user:String, val auction:String, val gmv:String, val seller:String, val gmvtime:String)  extends Serializable