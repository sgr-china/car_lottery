package com.sgr.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

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
