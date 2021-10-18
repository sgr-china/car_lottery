package com.sgr.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object SparkSQLEnv {

  var sparkContext: SparkContext = _
  var sparkSession: SparkSession = _

  val conf = new SparkConf(loadDefaults = true).setAppName("car").setMaster("local[*]")


  sparkSession = SparkSession.builder()
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  sparkContext = sparkSession.sparkContext
}
