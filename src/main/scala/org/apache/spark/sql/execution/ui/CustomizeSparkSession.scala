package org.apache.spark.sql.execution.ui

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.ui.CustomizeSparkSession.{QUEUE_ID, QUEUE_NAME, uuid}
import org.apache.spark.sql.execution.ui.customui.CustomizeTab
// import org.apache.spark.sql.execution.audit.LakeHouseSession.{QUEUE_ID, QUEUE_NAME, uuid}
// import org.apache.spark.sql.execution.audit.ui.AuditTab
// import org.apache.spark.sql.lakehouse.catalog.privileges.UserHolder
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.kvstore.KVStore

import java.util.UUID
import scala.reflect.ClassTag
/**
 * @author sunguorui
 * @date 2023年05月23日 5:52 下午
 */

class CustomizeSparkSession(val spark: SparkSession) {

  val sc: SparkContext = spark.sparkContext

  val kvStore: KVStore = sc.statusStore.store

  /**
   * Spark中的监听总线
   */
  val bus: LiveListenerBus = sc.listenerBus
  implicit val tag: ClassTag[CustomizeSQLListener] = ClassTag(classOf[CustomizeSQLListener])

  if (bus.findListenersByClass().isEmpty) {
    bus.addToQueue(new CustomizeSQLListener(spark, kvStore), QUEUE_NAME)
  }

  /**
   * web ui 展示
   */
  val ui: SparkUI = sc.ui.getOrElse(throw new RuntimeException("Not support web ui"))
  if (!ui.getTabs.map(_.prefix).contains("audits")) {
    ui.attachTab(new CustomizeTab(ui, new SQLCustomizeStore(kvStore, sc.conf)))
  }

  private var username: String = _
  private var cliHost: String = _

  /**
   * 设置当前用户名
   */
  def setUsername(username: String): Unit = {
    this.username = username
    setCurrentUsername(username)
  }

  /**
   * 设置当前的主机
   */
  def setCliHost(host: String): Unit = {
    this.cliHost = host
  }

  def sql(sql: String): DataFrame = {
    val df = spark.sql(sql)
    this.startExec(sql, df.queryExecution)
    df
  }

  private def startExec(sqlText: String, qe: QueryExecution): Unit = {
    val id = uuid
    sc.setLocalProperty(QUEUE_ID, id)
    bus.post(SQLExecutionStart(
      id, sqlText, qe, this.username, this.cliHost
    ))
  }

  private val CURRENT_USER: ThreadLocal[String] = new ThreadLocal[String]

  def getCurrentUsername: String = {
    CURRENT_USER.get()
  }

  def setCurrentUsername(username: String): Unit = {
    CURRENT_USER.set(username)
  }

  def clear(): Unit = {
    CURRENT_USER.remove()
  }
}

object CustomizeSparkSession {

  private val QUEUE_NAME = "Customize"

  val QUEUE_ID = "spark.sql.uuid"

  val CURRENT_SESSION_USER = "current.session.user"

  private def uuid: String = UUID.randomUUID().toString

}
