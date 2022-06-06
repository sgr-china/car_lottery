package org.apache.spark.util

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil

/**
 * @author sunguorui
 * 需要从SparkHadoopUtil中获取configuration配置信息，在spark3中将该类置为类private[spark]，所以使用这种方法曲线获取
 * @date 2022年03月03日 10:42 上午
 */
object GetSparkHadoopUtil {

  def getHadoopConf: Configuration = {
    SparkHadoopUtil.get.conf
  }

  def getNewConfiguration(sparkConf: SparkConf): Configuration = {
    SparkHadoopUtil.get.newConfiguration(sparkConf)
  }

}
