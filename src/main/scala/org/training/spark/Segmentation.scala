//package org.training.spark
//
//import org.apache.spark.rdd.RDD
//
///**
// * Created by zhangzhibo on 17-3-16.
// */
//class Segmentation {
//  //  def segmentation(pair : RDD ,len : Int): String = {
//  def segmentation(pair: RDD[(Long, Int)], i: Long, len: Int): String = {
//    var tmp: StringBuffer = null
//    for (j <- 0 until len) {
//      tmp = tmp.append(pair.lookup(i).head)
//    }
//    return tmp.toString
//
//  }
//}
