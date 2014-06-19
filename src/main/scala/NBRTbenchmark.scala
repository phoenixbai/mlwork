import org.apache.spark.mllib.classification.{NaiveBayesModel, NaiveBayes}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by phoenix on 4/3/14.
 */

object NBRTbenchmark {
  def main(args: Array[String]) {
//    if(args.size < 4) {
//      println("Usage: NBRTbenchmark master /input/path /test/path maxsize")
//      System.exit(0)
//    }
//    val input = args(1)
//    val sc = getSc(args(0), "NaiveBayesBenchmark")
//    val trainer = new NBRTbenchmark(args(3).toInt)
//    val (responseTime, model) = trainer.loadAndTrain(input, sc)
//    println("time spent in loadAndTrain: " + responseTime + "ms")
//    println("model obtained from nb train():" + model.toString)
//    val testData = args(2)
//    val testRDD = trainer.loadLabeledData(sc, testData)
//    println(testRDD.first())
//
//    // Test prediction on RDD.
//    validatePrediction(model.predict(testRDD.map(_.features)).collect(), testRDD.map(_.label).collect())

//    sc.stop()
  }

  def validatePrediction(predictions: Seq[Double], input: Seq[Double]) {
    val numOfPredictions = predictions.zip(input).count {
      case (prediction, expected) => {
//        println(prediction + " " + expected)
        prediction != expected
      }
    }
    // At least 80% of the predictions should be on.
    println("number of wrong predictions:" + numOfPredictions + " percent:" + numOfPredictions.toDouble/input.length)
  }

  def getSc(master:String, appname:String):SparkContext = {
    val conf = new SparkConf().setMaster(master)
                  .setAppName(appname)
                  .set("spark.executor.memory","2g")
    return new SparkContext(conf)
  }

}

class NBRTbenchmark(val MAXSIZE:Int) extends Serializable{
//  def loadAndTrain(input:String, sc:SparkContext) : (Long, NaiveBayesModel) = {
//    val startTime = System.currentTimeMillis()
//    val data = loadLabeledData(sc, input)
//    println(data.first())
//    val model = NaiveBayes.train(data)
//    val endTime = System.currentTimeMillis()
//    return ((endTime-startTime), model)
//  }

//  def loadLabeledData(sc: SparkContext, input:String) : RDD[LabeledPoint] = {
//    sc.textFile(input).map { line =>
//      val parts = line.split(" ",0)
//      var label = parts(0).toInt
//      if(label == -1)
//        label = 0
//      val featVals = new Array[Double](MAXSIZE)
//      for(item<-parts.slice(1,parts.size)) {
//        val kv = item.split(":")
////        println(kv(0) + " " + kv(1))
//        featVals.update(kv(0).toInt, kv(1).toDouble)
//      }
//      println(featVals(42633))
//      LabeledPoint(label, featVals)
//    }
//  }
}
