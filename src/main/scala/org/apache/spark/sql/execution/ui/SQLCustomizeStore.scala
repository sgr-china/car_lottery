package org.apache.spark.sql.execution.ui

import org.apache.spark.SparkConf

import org.apache.spark.sql.execution.ui.Configs.MAX_NUM_AUDIT_LOGS
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.status.KVUtils.KVIndexParam
import org.apache.spark.util.kvstore.KVStore

import scala.collection.JavaConverters._

/**
 * @author sunguorui
 * @date 2023年05月24日 10:59 上午
 */

class SQLCustomizeStore(store: KVStore, conf: SparkConf) {

  private val sqlAppStatusStore = new SQLAppStatusStore(store)

  private val kvStore: ElementTrackingStore = new ElementTrackingStore(store, conf)

  // 添加定期清理kvStore中的数据的触发器
  kvStore.addTrigger(classOf[SQLBaseData], conf.get(MAX_NUM_AUDIT_LOGS)) {
    count => cleanupBaseDatas(count)
  }

  def collectCustomizeData(): Seq[SQLCustomizeData] = {
    this.customizeBaseDataList()
      .map(joinMetricsData)
  }

  /**
   * 通过queryId的方式得到自定义的数据信息
   * @param queryId
   * @return
   */
  def getCustomizeData(queryId: String): Option[SQLCustomizeData] = {
    getDataByQueryId(queryId)
      .map(joinMetricsData)
  }

  /**
   * 通过ExecutionId来得到基础数据信息
   * @param executionId
   * @return
   */
  def getDataByExecutionId(executionId: Long): Option[SQLBaseData] = {
    store.view(classOf[SQLBaseData]).asScala
      .filter(_.executionId == executionId)
      .toSeq.headOption
  }

  def writeData(data: SQLBaseData): Unit = {
    // 写入一条数据，先获取数据的class，不存在则先创建class对应的数据列表InstanceList
    store.write(data)
  }

  def writeCoreData(data: SQLRelationWrapper): Unit = {
    // 写入一条数据，先获取数据的class，不存在则先创建class对应的数据列表InstanceList
    store.write(data)
  }

  def getRelation(id: String): Option[SQLRelationWrapper] = {
    try {
      // 获取某一class指定key数据，例如：store.read(classOf[SQLExecutionUIData], executionId)获取指定executionId的SQL运行描述detail
      Some(store.read(classOf[SQLRelationWrapper], id))
    } catch {
      case _: NoSuchElementException => None
    }
  }

  def metrics: Map[Long, Seq[SQLPlanMetric]] = {
    sqlAppStatusStore.executionsList()
      .map { i => (i.executionId, i.metrics) }
      .toMap
  }

  // 收集基础数据 后续做join
  def customizeBaseDataList(): Seq[SQLBaseData] = {
    // 返回的是KVStoreView的子类InMemoryView()，可以对InstanceList内部的数据进行排序，也就是我们在UI中看到的最新Stage信息总在最前
    store.view(classOf[SQLBaseData]).asScala.toSeq
  }

  /**
   * 与基础的信息做join 得到 增强后的信息
   * @param d
   * @return
   */
  private def joinMetricsData(d: SQLBaseData): SQLCustomizeData = {
    val builder = new SQLDataBuilder(d)
    this.sqlAppStatusStore.execution(d.executionId)
      .foreach(e => {
        // 获得监控信息 包括: executionId description details physicalPlanDescription metrics
        // submissionTime completionTime keyAs jobs contentAs stages metricValues
        val metricValues = this.sqlAppStatusStore.executionMetrics(e.executionId)
        val graphOption = this.getSparkGraphData(e.executionId)
        graphOption.foreach(g => {
          for (node <- g.nodes) {
            if (null != g.nodes) {
              findMetricValue(node.node.name, node.node.metrics, metricValues, builder)
            }
            if (null != node.cluster) {
              findMetricValue(node.cluster.name, node.cluster.metrics, metricValues, builder)
            }
          }
        })
      })
    builder.buildAuditData()
  }

  /**
   * 用来获取读取的行数
   * @param name SparkPlan计划树中的节点
   * @param metrics 包含如下信息:  name: String,accumulatorId: Long,metricType: String
   * @param metricValues 已得到的监控信息
   * @param builder 初步构造的数据信息
   */
  private def findMetricValue(name: String, metrics: Seq[SQLPlanMetric], metricValues: Map[Long, String],
                              builder: SQLDataBuilder): Unit = {
    name match {
      case "BatchScan" =>
        for (m <- metrics) {
          m.name match {
            case "number of output rows" =>
              metricValues.get(m.accumulatorId).foreach(v => builder.scanRows = v)
            case _ =>
          }
        }
      case _ =>
    }
  }

  /**
   * 包含信息用来和原始链接
   * @param executionId
   * @return SparkPlanGraphWrapper(executionId、SparkPlanGraphNodeWrapper、SparkPlanGraphEdge)
   */
  def getSparkGraphData(executionId: Long): Option[SparkPlanGraphWrapper] = {
    try {
      Some(store.read(classOf[SparkPlanGraphWrapper], executionId))
    }
  }

  def getDataByQueryId(queryId: String): Option[SQLBaseData] = {
    try {
      Some(store.read(classOf[SQLBaseData], queryId))
    } catch {
      case _: NoSuchElementException => None
    }
  }

  /**
   * 用来定期清理审计日志中的记录
   * @param maxNums
   */
  private def cleanupBaseDatas(maxNums: Long): Unit = {
    val count = store.count(classOf[SQLBaseData])

    if (count > maxNums) {

      val toDelete: Seq[SQLBaseData] = kvStore.view(classOf[SQLBaseData]).index("completionTime")
        .max(calculateNumberToRemove(count, maxNums)).first(0).last(System.currentTimeMillis()).asScala.toSeq
      toDelete.foreach(e => {
        kvStore.delete(classOf[SQLBaseData], e.queryId)
        kvStore.delete(classOf[SQLRelationWrapper], e.queryId)
      })
    }
  }

  /**
   * 具体清理多少条
   * @param dataSize
   * @param retainedSize
   * @return
   */
  private def calculateNumberToRemove(dataSize: Long, retainedSize: Long): Long = {
    if (dataSize > retainedSize) {
      math.max(retainedSize / 10L, dataSize - retainedSize)
    } else {
      0L
    }
  }

}

/**
 * SQL 关系存储适配类型
 *
 * @param queryId the query id
 * @param plan    the parsed logical plan
 */
class SQLRelationWrapper
(
  @KVIndexParam val queryId: String,
  val plan: String
)

