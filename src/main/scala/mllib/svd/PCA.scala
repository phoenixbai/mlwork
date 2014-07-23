package mllib.svd

import org.apache.spark.{SparkContext, SparkConf}
import org.jblas.{Eigen, DoubleMatrix}
import scala.util.Sorting

/**
 * Created by phoenix on 7/23/14.
 */

case class EigenInfo(index:Int, eigenVal:Double, eigenVec:DoubleMatrix)

object EigenOrdering extends Ordering[EigenInfo] {
  def compare(x:EigenInfo, y:EigenInfo) = {
    if(x.eigenVal < y.eigenVal) 1
    else if(x.eigenVal > y.eigenVal) -1
    else 0
  }
}

object PCA {

  val FieldSeparator = "\001"
  def main(args:Array[String]) {
    if(args.length<4) {
      println("Usage:PCA input/path colnum featnum partnum")
    }
    run(args)
  }
  def run(args:Array[String]) {

    val conf = new SparkConf().setAppName("Principal Component Analysis!")
    val sc = new SparkContext(conf)

    val input = args(0)
//    val output = args(1)
    val colnum = args(1).toInt   //total number of columns
    val featnum = args(2).toInt  //total number of features used in PCA
    val partnum = args(3).toInt
    //repartition the data
//    val data = sc.sequenceFile(input, classOf[String], classOf[org.apache.hadoop.io.Text]).map{ case(k,v) =>
    val data = sc.textFile(input).map{ v=>
        val fields = v.toString.split(FieldSeparator)
        fields
      }.filter(_.length==colnum).repartition(partnum).cache()

    val covarMatrix = new DoubleMatrix(featnum,featnum)  //initialize covariance matrix
    val diffnum = colnum - featnum  //non-feature column number
    val totalcount = data.count()

    for(i<-0 until featnum) {
      for(j<-0 until featnum) {
//        val x = data.map(_(i+diffnum).toDouble)
//        val y = data.map(_(j+diffnum).toDouble)
        val xy = data.map(v => (v.apply(i+diffnum).toDouble, v.apply(j+diffnum).toDouble))
        val covar = xy.map(v => v._1*v._2).reduce((x,y) => x+y)/(totalcount-1)
        covarMatrix.put(i,j,covar)
      }
    }
    //calculate eigen vector
    val eigen = calcEigenVector(covarMatrix)
    val eigenVectors = eigen(0)
    val eigenValues = eigen(1)

    val len = eigenValues.getColumns
    var eigenArray = Array.empty[EigenInfo]
    for(i<-0 until len) {
      eigenArray :+= new EigenInfo(i,eigenValues.diag().get(i),eigenVectors.getColumn(i))
    }
    Sorting.quickSort(eigenArray)(EigenOrdering)
    println("printing eigen value & vectors!!!")
    for(j<-0 until len) {
      println(eigenArray.apply(j))
    }
    sc.stop()
  }

  def calcEigenVector(mat: DoubleMatrix): Array[DoubleMatrix] = {
    val eigen = Eigen.eigenvectors(mat).map(_.real())
    println("==== eigen vectors ====" + eigen.apply(0))
    println("==== eigen values ====" + eigen.apply(1))
    eigen
  }
}
