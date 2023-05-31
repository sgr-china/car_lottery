package org.apache.spark.sql.execution.ui

import com.sgr.util.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.execution.ui.Configs.{MAX_NUM_AUDIT_LOGS, REPORT_AUDIT_LOG_INTERVAL_MS}
import org.sparkproject.guava.cache.{Cache, CacheBuilder}

import java.util.concurrent.TimeUnit
import scala.collection.mutable

/**
 * @author sunguorui
 * @date 2023年05月29日 11:06 上午
 */

class CustomizeReportManager(val customizeStore: SQLCustomizeStore, val conf: SparkConf) extends LazyLogging {

  private val _reporters = mutable.ListBuffer[CustomizeDataReporter]()

  private val thread: Thread = new Thread(() => {
    while (conf.get(REPORT_AUDIT_LOG_INTERVAL_MS) > 0) {
      try {
        report()
      } catch {
        case e: Exception =>
          logger.info("The report thread error", e)
      }
      Thread.sleep(conf.get(REPORT_AUDIT_LOG_INTERVAL_MS))
    }
  }, "ReportThread")

  /**
   * 注册上报设备
   *
   * @param reporter 上报设备
   */
  def register(reporter: CustomizeDataReporter): Unit = {
    _reporters += reporter
  }

  def asyncStart(): Unit = {
    this.thread.start()
  }
  def report(): Unit = {
    val data = customizeStore.collectCustomizeData()
      .filter(d => Byte.MaxValue == reportedMap.get(d.queryId, () => Byte.MinValue))
      .map(d => {
        reportedMap.put(d.queryId, Byte.MaxValue)
        d
      })
      .sortWith((a, b) => a.queryStart.getTime > b.queryStart.getTime)
    _reporters.foreach(r => r.invoke(data))
  }

  private val reportedMap: Cache[String, java.lang.Byte] = CacheBuilder.newBuilder()
    .expireAfterAccess(30, TimeUnit.MINUTES)
    .maximumSize(conf.get(MAX_NUM_AUDIT_LOGS))
    .build[String, java.lang.Byte]()


}

abstract class CustomizeDataReporter {

  def invoke(data: Seq[SQLCustomizeData]): Unit

}

/**
 * 控制台上报设备
 */
class ConsoleCustomizeDataReporter
  extends CustomizeDataReporter with LazyLogging {
  override def invoke(dataList: Seq[SQLCustomizeData]): Unit = {
    dataList.foreach(d => {
      logger.debug(s"SQL AUDIT: " +
        JsonUtils.toJson(d)
      )
    })
  }
}
