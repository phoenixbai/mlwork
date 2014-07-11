import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks
/**
 * Created by phoenix on 4/2/14.
 */

object hello1 {
  def main(){
    var fn1 = 55
    var fn2 = 34
    var fn = 0
    for(i<-0  until 100){
      fn = fn1+fn2
      println(fn)
      fn2=fn1
      fn1=fn
    }
  }
}

hello1.main()




































def hello {
  var fn1 = 55
  var fn2 = 34
  var fn = 0
  for(i<-0  until 100){
    fn = fn1+fn2
    println(fn)
    fn2=fn1
    fn1=fn
  }
}







