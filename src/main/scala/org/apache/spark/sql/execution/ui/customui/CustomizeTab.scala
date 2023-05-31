package org.apache.spark.sql.execution.ui.customui

import org.apache.spark.sql.execution.ui.SQLCustomizeStore
import org.apache.spark.ui.{SparkUI, SparkUITab}

/**
 * @author sunguorui
 * @date 2023年05月24日 10:58 上午
 */

class CustomizeTab(parent: SparkUI, store: SQLCustomizeStore)
  extends SparkUITab(parent, "Customize") {

  attachPage(new CustomizePage(this, store))
  attachPage(new CustomizeDescPage(this, store))

}
