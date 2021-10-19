package com.sgr.other

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Test1 {

  def main(args: Array[String]): Unit = {
    // 这里的下划线"_"是占位符，代表数据文件的根目录
    val rootPath: String = "/Users/guorui"
    val file: String = s"${rootPath}/wikiOfSpark.txt"

    val conf = new SparkConf().setAppName("1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // 读取文件内容
    val rdd: RDD[String] = sc.textFile(file)
    // 以行为单位做分词
    val rddSpilt = rdd.flatMap(line => line.split(" "))
    // 定义Long类型的累加器
    val ac = sc.longAccumulator("Empty string")

    // 定义filter算子的判定函数f，注意，f的返回类型必须是Boolean
    def f(x: String): Boolean = {
      if (x.equals("")) {
        // 当遇到空字符串时，累加器加1
        ac.add(1)
        return false
      } else {
        return true
      }
    }

    // 使用f对RDD进行过滤
    val cleanWord: RDD[String] = rddSpilt.filter(f)

    val kvRDD: RDD[(String, Int)] = cleanWord.map(word => (word, 1))

    val wordCount: RDD[(String, Int)] = kvRDD.reduceByKey((x, y) => x + y)

    wordCount.collect
    // scalastyle:off println
    println(ac.value)



    /*    val rddFilter = rddSpilt.filter( word => !word.equals(" "))
    // 把RDD元素转换为（Key，Value）的形式
    val mapRDD = rddFilter.map(word => (word, 1))
    // 按照单词做分组计数
    val reRdd = mapRDD.reduceByKey((x, y) => x + y)
    // 打印词频最高的5个词汇
    val a = reRdd.map{ case (k , v) => (v, k)}.sortByKey(false).take(5)
    a.foreach {x =>
      println(x)
    } */

  }

}
