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
    if(args.length < 3) {
      println("Usage: KmeansPrice master input output")
    }
    val conf = new SparkConf().setMaster(args(0))
                              .setAppName("clustering auctions based on price!")

    val sc = new SparkContext(conf)
    val input = args(1)
    val output = args(2)
    var filetype = 1
    if(args.length>3)
        filetype = args(3).toInt

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

      if (ldataCount > 100) clusterNum = 10
      else if (ldataCount <=100 && ldataCount > 20) clusterNum = 5
      else if (ldataCount <=20 && ldataCount>5) clusterNum = 2
      else clusterNum = 1

      val prices = leafdata.map(x => Vectors.dense(x.price))
      val clusters = KMeans.train(prices, clusterNum, iterations)
      val predicts = clusters.predict(prices)
      leafdata.zip(predicts)
    }.reduce(_.union(_))

    /* add below logic here

    create table cat_clusters as
 select category, cat1, cid, count(brand_id) as cnt, min(weighted_pay) as mi, max(weighted_pay) as ma
 from brand_clusters
 group by category, cat1, cid;

  drop table brand_clusters_final;
 create table brand_clusters_final as
 select a.brand_id, a.category, a.cat1, a.weighted_pay, b.new_cid, b.mi, b.ma
 from brand_clusters a
 join (select category, cat1, cid, mi, ma, cnt, row_number(category, cat1) as new_cid
       from (select category, cat1, cid, mi, ma, cnt
             from cat_clusters
             distribute by category, cat1
             sort by category, cat1, mi desc ) t
       ) b
on (a.category=b.category and a.cat1=b.cat1 and a.cid=b.cid);
     */

//    predictData.cogroup()

    predictData.map(v => ((v._1.leafcat, v._2), v._1.price)).groupByKey()
      .map { case (k, v) => (k._1, (k._2, v.toSeq.min, v.toSeq.max)) }
      .groupByKey().flatMap {
        case (k, v) => {
          val a = v.toSeq.sortBy(_._2).reverse
          val b = a.zip(1 to a.length)
          b.map((k,v)=>(k._1, k._2, k._3, v))
        }
    }

    val catcdata = new PairRDDFunctions[(String,Int),Double](predictData.map(v => ((v._1.leafcat, v._2),v._1.price)))
    val catgmin = catcdata.reduceByKey((a,b) => Math.min(a,b))
    val catgmax = catcdata.reduceByKey((a,b) => Math.max(a,b))
    val catClusters = catgmin.cogroup(catgmax)

    catClusters.groupByKey().flatMap()



    val result = predictData.repartition(1).map( v =>
      v._1.brand +"," + v._1.leafcat +"," + v._1.cat1 +"," + v._1.price+ "," + v._2)
    result.saveAsTextFile(output)
  }


}
