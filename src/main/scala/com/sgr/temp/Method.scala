package com.sgr.temp

import com.sgr.service.HDFSService
import com.sgr.spark.SparkSQLEnv
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.execution.datasources.LogicalRelation


/**
 * @author sunguorui
 * @date 2022年03月02日 8:25 下午
 */
object Method {
  SparkSQLEnv.init()
  private val sparkSession = SparkSQLEnv.sparkSession
  private val hdfs = new HDFSService
  val dbTable = "spark_project.user_info"

  def main(args: Array[String]): Unit = {

    // scalastyle:off println
    println(getCatalogTable(dbTable))
  }

  /**
   * ArrayBuffer(FileStatus(/user/hive/warehouse/spark_project.db/
   * user_info/part-00000-db108ed9-b17a-4541-a7f4-3a259dbc3072-c000.snappy.parquet,2072)
   * FileStatus(/user/hive/warehouse/spark_project.db/
   * user_info/part-00001-db108ed9-b17a-4541-a7f4-3a259dbc3072-c000.snappy.parquet,2095)
   * @param dbtable
   * @return
   */
  def listTableFiles(dbtable: String): Seq[FileStatus] = {
    val path = getTableDataPath(dbtable)
    hdfs.listFiles(path)
      .map(f => FileStatus(f.getPath.toUri.getPath, f.getLen))
      .sortBy(_.path)

  }

  /**
   * 获取一个表的 hdfs 路径
   * /user/hive/warehouse/spark_project.db/user_info
   * @param dbtable
   * @return
   */
  def getTableDataPath(dbtable: String): String = {
    getCatalogTable(dbtable).location.getPath
  }

  case class FileStatus(path: String, length: Long) {
    val name: String = path.substring(path.lastIndexOf("/") + 1)
  }

  /**
   * sschema、hdfsPath  如果是视图的话viewText表示Some(select * from user_info)
   * CatalogTable(
Database: spark_project
Table: user_info
Owner: guorui
Created Time: Tue Oct 26 20:34:14 CST 2021
Last Access: UNKNOWN
Created By: Spark 3.0.3
Type: MANAGED
Provider: parquet
Statistics: 16792 bytes
Location: hdfs://localhost:8020/user/hive/warehouse/spark_project.db/user_info
Serde Library: org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
InputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
OutputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
Schema: root
 |-- user_id: long (nullable = true)
 |-- username: string (nullable = true)
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- professional: string (nullable = true)
 |-- city: string (nullable = true)
 |-- sex: string (nullable = true)
)
   * @param dbtable
   * @return
   */
  def getCatalogTable(dbtable: String): CatalogTable = {
    val table = sparkSession.sessionState.sqlParser.parseTableIdentifier(dbtable)
    sparkSession.sessionState.catalog.getTableMetadata(table)
  }



  /**
   * ArrayBuffer(`spark_project`.`user_info`)
   * 返回视图所依赖的库.表
   * @param tableName
   * @return
   */
  def getHiveTableFromView(tableName: String): Seq[String] = {
    sparkSession.sql("use spark_project")
//    sparkSession.sql("create view sgr_test as select * from user_info")
    val viewText = getCatalogTable(tableName).viewText
    if (viewText.isDefined) {
      sparkSession.sql(viewText.get)
        .queryExecution.analyzed
        // 返回一个Seq，该Seq包含对定义该函数的树中的所有元素应用分部函数的结果。
        .collect{
          case HiveTableRelation(tableMeta, _, _, _, _) => tableMeta.identifier.toString()
          case LogicalRelation(_, _, Some(tableMeta), _) => tableMeta.identifier.toString()
        }.distinct
    } else {
      Nil
    }
  }



}
