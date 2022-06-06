package com.sgr.spark

import com.sgr.util.Conf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.GetSparkHadoopUtil


/**
 * @author sunguorui
 * @date 2022年03月03日 10:19 上午
 */
object SparkSQLEnv {

  var sparkContext: SparkContext = _
  var sparkSession: SparkSession = _
  var hadoopConfig: org.apache.hadoop.conf.Configuration = _

  def init(): Unit = {
    val sparkConf = new SparkConf(loadDefaults = true).setAppName("car").setMaster("local[*]")
    // 从spark-extra.conf读取配置
    val sparkExtraProp = Conf.parseSparkConf()

    for ((k, v) <- sparkExtraProp) {
      // scalastyle:off println
      println(s"sparkExtraConf: key -> $k  value -> $v")
      // scalastyle:on println
      sparkConf.set(k, v)

    }
    sparkSession = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    sparkContext = sparkSession.sparkContext
    hadoopConfig = GetSparkHadoopUtil.getNewConfiguration(sparkConf)
  }


}
