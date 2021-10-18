package com.sgr.util

import org.apache.spark.sql.DataFrame

class ShowData extends LazyLogging{

  val sparkSession = SparkSQLEnv.sparkSession

  val rootPath: String = "/car_data"

  val hdfs_path_apply: String = s"${rootPath}/apply"
  val applyNumbersDF: DataFrame = sparkSession.read.parquet(hdfs_path_apply)
  logger.info("apply information")
  logger.error("apply information")
  applyNumbersDF.show()

  val hdfs_path_lucky: String  = s"${rootPath}/lucky"
  val luckyNumbersDf: DataFrame = sparkSession.read.parquet(hdfs_path_lucky)
  logger.info("lucky information")
  applyNumbersDF.show()
}
