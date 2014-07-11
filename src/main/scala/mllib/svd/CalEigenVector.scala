package mllib.svd

import org.apache.spark.{SparkContext, SparkConf}
import org.jblas.{Eigen, DoubleMatrix}

/**
 * Created by phoenix on 7/11/14.
 */
object CalEigenVector {

  def main(args:Array[String]) {
    run(args)
  }
  def run(args:Array[String]) {

    val input = args(0)
//    val output = args(1)
    val featnum = args(1).toInt   //特征数

    val conf = new SparkConf().setAppName("Calculate Eigen Vector!")
    val sc = new SparkContext(conf)

//    val clazz = Seq(classOf[String], classOf[Double])

    val data = sc.textFile(input).map { line =>
      val fields = line.split("\001") //auctionId,cat,uv,collect,cart,sale,compl,refund,rpos,rneg,ravg,prop,npx
//      (fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6), fields(7),fields(8),
//        fields(9), fields(10), fields(11), fields(12))
      fields
    }.cache()
    val rowcount = data.count()
    val covarMatrix = new DoubleMatrix(featnum, featnum)
    //constructing covariance matrix
    for(i<-0 until featnum)
      for (j<-0 until featnum) {
        val col1 = data.map(_(i+2).toDouble)
        val col2 = data.map(_(j+2).toDouble)
        val covar = col1.zip(col2).map( x => x._1 * x._2).reduce((x,y) => x+y)/(rowcount-1)

        covarMatrix.put(i,j,covar)
//        clazz(i) match {
//          case String =>
//          case Double =>
//        }
      }
    println("==== covariance matrix ====")
    for(i<-0 until covarMatrix.getColumns)
      println(covarMatrix.getColumn(i))
//    println(covarMatrix.toString())
    val eigen = calcEigenVector(covarMatrix)
    println("==== eigen vectors ====")
    for(i<-0 until eigen(0).getColumns)
      println(eigen(0).getColumn(i))
    println("==== eigen values ====")
    for(i<-0 until eigen(1).getColumns)
      println(eigen(1).diag().get(i))
    sc.stop()

  }

  def calcEigenVector(mat: DoubleMatrix): Array[DoubleMatrix] = {
    val eigen = Eigen.eigenvectors(mat).map(_.real())
    println("==== eigen vectors ====" + eigen.apply(0))
    println("==== eigen values ====" + eigen.apply(1))
    eigen
//    for (i <- 0 until eigen(0).getColumns) {
//      val col = eigen(0).getColumn(i)
//      val value = eigen(1).diag().get(i)
//      if (math.abs(value) > 1e-6) {
//        println("value: " + value + ", vector: " + col + ", sum: " + col.sum())
//        //        println("validate: " + mat.mmul(col) + "\n\t" + col.mul(value))
//        return col
//      }
//    }
//    null
  }
}
