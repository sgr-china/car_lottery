package com.sgr.util

import com.sgr.spark.SparkSQLEnv
import org.apache.spark.sql.DataFrame

object GetData extends LazyLogging{
  // 表
  // apply  2011-2019 年各个批次参与摇号的申请号码
  // lucky 各个批次中签的申请号码

  // 列
  // carNum 申请号码
  // batchNum 摇号批次

  private val sparkSession = SparkSQLEnv.sparkSession

  private val rootPath: String = Conf.rootPath

  private val hdfs_path_apply: String = s"${rootPath}/apply"
  val applyNumbersDF: DataFrame = sparkSession.read.parquet(hdfs_path_apply)
//  logger.info("apply information")
//  applyNumbersDF.show()

  private val hdfs_path_lucky: String = s"${rootPath}/lucky"
  val luckyNumbersDf: DataFrame = sparkSession.read.parquet(hdfs_path_lucky)
//  logger.info("lucky information")
//  applyNumbersDF.show()
}
