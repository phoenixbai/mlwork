
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.NaiveBayes
import org.junit.Ignore

/**
 * Created by phoenix on 3/25/14.
 */

object NaiveBayesTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local")
                              .setAppName("NaiveBayes")
                              .set("spark.executor.memory","2g")
                              .set("spark.storage.memoryFraction", "0.2")
    //val sc = new SparkContext("spark://phoenixbai:7077", "NaiveBayes")
    val sc = new SparkContext(conf)
    val startTime = System.currentTimeMillis()
    val data = MLUtils.loadLabeledData(sc, "/home/phoenix/dataset/classification/diabetes.rdd")
    println(data.first())
    val model = NaiveBayes.train(data)
    val endTime = System.currentTimeMillis()
    println("time spent in training classifier:" + (endTime-startTime) + "ms")
    println("Pi: " + model.pi.mkString("[", ", ", "]"))
    println("Theta:\n" + model.theta.map(_.mkString("[", ", ", "]")).mkString("[", "\n ", "]"))
    //predict the class of new instance using obtained model
    val testdata = MLUtils.loadLabeledData(sc, "/home/phoenix/dataset/classification/diabetes_test.rdd")
//    val c = model.predict(Array(1.000000,93.000000,70.000000,31.000000,0.000000,30.400000,0.315000,23.000000))

//    println(c)

    sc.stop()
  }
}

