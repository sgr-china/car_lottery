package com.sgr.service

import com.sgr.spark.SparkSQLEnv
import com.sgr.temp.Method.getCatalogTable
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import scala.collection.mutable.ArrayBuffer

/**
 * @author sunguorui
 * @date 2022年03月03日 11:05 上午
 */
class HDFSService {





  def listFiles(srcDir: String): Seq[FileStatus] = {
    val path = new Path(srcDir)
    listFiles(path)
  }

  /**
   * 获取一个目录下所有的parquet文件
   * @param srcDir
   * @return ArrayBuffer(FileStatus{path=hdfs://localhost:8020/user/hive/warehouse/
   *         spark_project.db/user_info/part-00000-db108ed9-b17a-4541-a7f4-3a259dbc3072-c000.snappy.parquet;
   * isDirectory=false; length=2072; replication=1; blocksize=134217728; modification_time=1635251653738;
   * access_time=1637824242446; owner=guorui; group=supergroup;
   * permission=rw-r--r--; isSymlink=false}
   */
  def listFiles (srcDir: Path): Seq[FileStatus] = {
    val config = SparkSQLEnv.hadoopConfig
    val fs = FileSystem.get(config)
    val listing = fs.listStatus(srcDir)
    val files = new ArrayBuffer[FileStatus]()
    for (file <- listing) {
      if (!file.getPath.getName.startsWith(".")
        && !file.getPath.getName.startsWith("_")) {
        if (file.isDirectory) {
          files.append(listFiles(file.getPath): _*)
        } else {
          files.append(file)
        }
      }
    }
    files
  }
}
