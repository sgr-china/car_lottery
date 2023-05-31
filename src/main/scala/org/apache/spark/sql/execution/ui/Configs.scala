package org.apache.spark.sql.execution.ui

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}

/**
 * @author sunguorui
 * @date 2023年05月24日 9:09 下午
 */
object Configs {
  /**
   * 表示保存的最大审计日志条数
   *
   * 若自从上次审计日志上报完成后，直到下一次审计日志上报之前。期间新产生的审计日志条数超过 {@link MAX_NUM_AUDIT_LOGS}
   * 则新产生的审计日志中，最早的审计日志则被丢弃，即数据会丢失
   */
  val MAX_NUM_AUDIT_LOGS: ConfigEntry[Long] = ConfigBuilder("spark.lakehouse.audit.maxLogNums")
    .version("3.2.1")
    .longConf
    .createWithDefault(10 * 10000)

  /**
   * 表示两次审计日志上报的时间间隔，默认 15 s
   */
  val REPORT_AUDIT_LOG_INTERVAL_MS: ConfigEntry[Long] = ConfigBuilder("spark.lakehouse.audit.intervalReportMs")
    .version("3.2.1")
    .longConf
    .createWithDefault(15 * 1000)

}
