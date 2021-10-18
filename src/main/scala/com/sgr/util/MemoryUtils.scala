package com.sgr.util

import java.lang.management.ManagementFactory

/**
 * @author sunguorui
 * @date 2021年10月18日 8:13 下午
 */
object MemoryUtils {

  def getMemUsedPercent: Int = {
    val heapMemoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage()
    (heapMemoryUsage.getUsed * 100.0 / heapMemoryUsage.getCommitted).toInt
  }
}
