package org.training.spark

;

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.tools.cmd.gen.AnyVals.L

/**
 * Hello world!
 *
 */
object App {
  def main(args: Array[String]) {


    val conf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(conf)
    val file = sc.textFile("/home/zhangzhibo/IdeaProjects/wordcount/data.csv")
    //  val file = sc.textFile("hdfs://master:9000/user/hadoop/modedata/data2.csv")

    val lines = file.flatMap(line => line.split(","))

    val nums = lines.filter(line => !line.contains("/"))
    val pair = nums.zipWithIndex()
    val len = lines.count()
    val newpair1 = pair.map(pa => (pa._2, pa._1.toLong))
    val newpair2 = pair.map(pa => (pa._2 - 1, pa._1.toLong))
    val tmp1 = newpair1.filter { case (key, value) => (key < len) }
    val tmp2 = newpair2.filter { case (key, value) => (key > -1) }

    val re = tmp1.join(tmp2);

    def code(x: Long, y: Long): Int = {
      if (x - y > 0) 2
      else if (x - y < 0) 1
      else 0
    }
    def rddtoString(s1: String, s2: String): String = {
      val str: String = s1 + s2
      return str
    }
    val resault = re.mapValues((p => code(p._1, p._2)))
    //    val edit =
    //    def segmentation(pair: RDD[(Long, Int)], i: Long, len: Int): String = {
    //      var tmp: StringBuffer = null
    //      for (j <- 0 until len) {
    //        tmp = tmp.append(pair.lookup(i).head)
    //      }
    //      return tmp.toString
    //
    //    }

    //    tmpresault  -> RDD(Long,String)
    val tmpresault = resault.mapValues(v => v.toString)
    //    val rddarry = new Array[RDD[(Long, Int)]](5)
    //    for (i <- 1 until 5) {
    ////      tt  -> RDD(Long,String)
    //      var tt = tmpresault.map(pa => (pa._1 - i, pa._2))
    //      //      tmpresault = tmpresault.join(tmpresault.map(pa => (pa._1 - i, pa._2)))//.mapValues((p => rddtoString(p._1, p._2)))
    //      tmpresault = tmpresault.join(tt).mapValues((p => rddtoString(p._1, p._2)))
    ////      tmpresault = tmpresault.mapValues((p => rddtoString(p._1, p._2)))
    //      tmpresault = tmpresault.sortByKey()
    //      tmpresault.foreach(println)
    //    }
    val rddarray = new Array[RDD[(Long, String)]](6)
    for (i <- 1 until 7) {
      rddarray(i - 1) = tmpresault.map(pa => (pa._1 - i, pa._2))
    }
    var tmp = tmpresault
    for (i <- 0 until 6) {
      tmp = tmp.join(rddarray(i)).mapValues((p => rddtoString(p._1, p._2)))
    }
    tmp = tmp.sortByKey()
    tmp.foreach(println)
    println(tmp.count())
    resault.filter { case (key, value) => (key < len - 4) }

    println("===================================")
    //    resault.take(10).foreach(println)
    //    val seg = new Segmentation
    //    val findresault = resault
    //    val segpair = resault.map(pa => (segmentation(resault, pa._1, 10)))
    //    segpair.take(10).foreach(println)
    //  val segpair = pair.map(_ =>seg.segmentation(pair,) )
    val edit = new Edit
    val str1 = "abc"
    val str2 = "abd"
    val stest = "1000222"
    var ma = tmp.mapValues(str => edit.editdistance(str, stest))
    ma = ma.sortByKey()
    ma.foreach(println)
    val similiraty: Float = edit.editdistance(str1, str2)
    println(similiraty)

    ////  pair.take(10).foreach(println)
    ////  tmp1.take(10).foreach(println)
    ////  tmp2.take(10).foreach(println)
    //  val laste = resault.sortByKey()
    //  laste.take(10).foreach(println)
    //  print(laste.lookup(1).head)
  }
}