import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by phoenix on 4/3/14.
 */

object BenchmarkTest {
  def main(args: Array[String]) {
    val MAXSIZE = 47236
    val input = args(1)  //"/home/phoenix/dataset/classification/diabetes.rdd"
    val conf = new SparkConf().setMaster(args(0))
      .setAppName("NaiveBayesBenchmarkTest")
      .set("spark.executor.memory","2g")
      .set("spark.storage.memoryFraction", "0.2")
    val sc =  new SparkContext(conf)
    val file = sc.textFile(input)

    val dataRDD = sc.textFile(input).map { line =>
      val parts = line.split(",")
      val label = parts(0).toInt
      val features = parts(1).trim().split(" ")
      val featVals = new Array[Double](MAXSIZE)
      for(item<-features) {
        val kv = item.split(":")
        println(kv(0) + " " + kv(1))
        featVals.update(kv(0).toInt, kv(1).toDouble)
      }
      println(featVals(42633))
//      LabeledPoint(label, featVals)
    }
//    println(dataRDD.first())
//      val line = "0,2:1 3:1 4:1 10:1 11:1 14:1 19:1 38:1 39:1 40:1 41:1 42:1 43:0.301 44:0.301 48:1 31690:1 31691:0.301 31693:1 31696:1 31712:1 31713:1 31714:1 2014670:1 2014671:1 2014672:1"
//      val parts = line.split(',')
//      val label = parts(0).toDouble
//      val darray = new Array[Float](MAXSIZE)
//    val vals = parts(1).split(" ")
//    val iter = vals.toIterable
//
//    for(item <- vals;if(item.split(":").size>1)) {
//      println(item)
//      val kv = item.split(":")
//      darray.update(kv(0).toInt, kv(1).toFloat)
//    }
//    println(darray(100))
//      parts(1).trim().split(' ').map { kv =>
//        darray.update(vals(0).toInt, vals(1).toFloat)
//      }
//      println(features)
//    }

    sc.stop()

  }

}
