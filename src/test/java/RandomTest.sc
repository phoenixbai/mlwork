import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

/**
 * Created by phoenix on 4/2/14.
 */

val points = Seq(
  Vectors.dense(0.0, 0.0),
  Vectors.dense(0.0, 0.1),
  Vectors.dense(0.1, 0.0),
  Vectors.dense(9.0, 0.0),
  Vectors.dense(9.0, 0.2),
  Vectors.dense(9.2, 0.0)
)

val a = 0xff000000 | -13684945
println(a)

0 >> 0

