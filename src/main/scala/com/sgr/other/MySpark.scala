package com.sgr.other

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object MySpark {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sgr_test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().getOrCreate()

    sc.broadcast()


    val list: List[String] = List("spark", "abc")
    val bc = sc.broadcast(list)


    println(bc.value)

    /* val rdd =sc.parallelize(List(1,2,3,4,5,6)).map(_*3)
    val mappedRDD=rdd.filter(_>10).collect() //对集合求和
    println(rdd.reduce(_+_)) //输出大于10的元素
    for(arg <- mappedRDD)
      print(arg+" ")
    println()
    println("math is work") */
  }

  /*def getValue = {
    val conf = new SparkConf().setAppName("sgr_test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd =sc.parallelize(List(1,2,3,4,5,6)).map(_*3)
    val mappedRDD=rdd.filter(_>10).collect() //对集合求和
    println(rdd.reduce(_+_)) //输出大于10的元素
    for(arg <- mappedRDD)
      print(arg+" ")
    println()
    println("math is work")
  }*/

}
