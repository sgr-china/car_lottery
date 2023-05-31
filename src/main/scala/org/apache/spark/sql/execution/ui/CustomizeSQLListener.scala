package org.apache.spark.sql.execution.ui

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerJobStart}
import org.apache.spark.util.kvstore.KVStore
import org.apache.spark.sql.SparkSession
import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{ExplainMode, QueryExecution, SQLExecution}

import scala.collection.JavaConverters._
import java.util.concurrent.ConcurrentHashMap
/**
 * @author sunguorui
 * @date 2023年05月23日 6:07 下午
 */

class CustomizeSQLListener(val spark: SparkSession, val kvStore: KVStore) extends SparkListener {
  // 自定义的kvStore 加入了定时清理的触发器(kvStore.addTrigger) 加入了自定义的数据（sql、plan等）采用join的方式 sqlAppStatusStore.execution
  // sqlAppStatusStore.executionMetrics/sqlAppStatusStore.executionMetrics
  private val customizeStore = new SQLCustomizeStore(kvStore, spark.sparkContext.conf)
  // 数据上报线程
  private val reportManager = new CustomizeReportManager(customizeStore, spark.sparkContext.conf)

  reportManager.register(new ConsoleCustomizeDataReporter)
  reportManager.register(new DeltaCustomizeDataReporter(spark))
  reportManager.asyncStart()

  private val executionDatas = new ConcurrentHashMap[Long, SQLDataBuilder]()

  // queryId -> SQLRelation
  private val coreDatas = new ConcurrentHashMap[String, SQLRelation]()

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SparkListenerSQLExecutionStart =>

        val builder: SQLDataBuilder = this.getOrCreateExecutionData(e.executionId)

        builder.queryStart = Math.min(builder.queryStart, e.time)
        builder.associatedExecutionIds += e.executionId
        builder.applicationName = spark.conf.get("spark.app.id")
        this.executionDatas.put(e.executionId, builder)


      case e: SparkListenerSQLExecutionEnd =>

        val builder: SQLDataBuilder = this.getOrCreateExecutionData(e.executionId)

        builder.associatedExecutionIds += e.executionId
        builder.queryEnd = Math.max(builder.queryEnd, e.time)
        builder.duration = builder.queryEnd - builder.queryStart
        builder.parsedLogicalPlan = e.qe.analyzed.toString()
        builder.optimizedLogicalPlan = e.qe.optimizedPlan.toString()
        val mode = ExplainMode.fromString(spark.sessionState.conf.uiExplainMode)
        builder.physicalPlan = e.qe.explainString(mode)
        builder.physicalReads = e.qe.executedPlan.children.size
        builder.status = e.executionFailure.isEmpty
        builder.message = e.executionFailure.map(_.getMessage).orNull
        builder.setPlanDesc(this.parserLogicalPlan(e.qe.analyzed))
        builder.finished = true

        this.executionDatas.put(e.executionId, builder)
        // 将这里构建的SQLDataBuilder中的数据写入kvStore
        this.update()
      case e: SQLExecutionStart =>
        // Save core execution information
        val d = this.getOrCreateRelation(e.queryId)
        d.plan = e.queryExecution.analyzed.toString()
        d.sql = e.originalSql
        d.username = e.username
        d.clientAddress = e.clientAddress

        this.coreDatas.put(e.queryId, d)

      case _ =>
    }
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    val properties = event.properties
    val queryId: String = properties.getProperty(CustomizeSparkSession.QUEUE_ID)
    val executionId = properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
    if (null == queryId || null == executionId) {
      return
    }

    val id = executionId.toLong

    val data: SQLRelation = this.getOrCreateRelation(queryId)
    data.associatedExecutionIds += id
    this.coreDatas.put(queryId, data)

  }

  private def getOrCreateRelation(id: String): SQLRelation = {
    this.coreDatas
      // 得到SQLRelationWrapper(queryId, parsed logical plan)
      .computeIfAbsent(id, id => this.customizeStore.getRelation(id)
      .map(d => new SQLRelation(d.queryId, d.plan))
      .getOrElse(new SQLRelation(id)))
  }

  /**
   * 将构造的SQL数据写入kvStore
   */
  private def update(): Unit = {
    for (data <- this.coreDatas.values.asScala) {
      for (exec <- this.executionDatas.values.asScala) {
        // 当两个parsedLogicalPlan相同时，关联数据, 将sql添加了进去
        if (exec.finished && (StringUtils.isNotEmpty(exec.queryId)
          && StringUtils.equals(exec.queryId, data.queryId)
          || StringUtils.isEmpty(exec.queryId)
          && StringUtils.equals(data.plan, exec.parsedLogicalPlan))) {
          // 加入了SQL
          exec.queryId = data.queryId
          exec.originalSql = data.sql
          // 将SQLBaseData写入kvStore
          customizeStore.writeData(exec.buildBaseData())
          // 将queryId和plan构成的对象写入kvStore
          customizeStore.writeCoreData(new SQLRelationWrapper(data.queryId, data.plan))
          // 写入完成后就可以从集合中将对应的queryId的数据删除
          this.executionDatas.remove(exec.executionId)
          this.coreDatas.remove(data.queryId)
        }
      }
    }
  }

  /**
   * 从logicalPlan中解析 逻辑计划详细信息(读的表、写的表、命令类型)
   * @param plan
   * @return
   */
  private def parserLogicalPlan(plan: LogicalPlan): LogicalPlanDesc = {
    val res = new LogicalPlanDesc
    PlanDescConvertor.convertPlanDesc(plan, res)
    res
  }

  /**
   * 从集合中取对应executionId的SQLBaseData
   * 如果存在则返回不存在则从kvStore中查询并创建
   * @param id
   * @return
   */
  private def getOrCreateExecutionData(id: Long): SQLDataBuilder = {
    this.executionDatas
      // 根据id从executionDatas中取数据取到则返回 取不到则按照后续的func创建并插入到集合中
      // 拿到指定id的SQLBaseData数据
      .computeIfAbsent(id, id => this.customizeStore.getDataByExecutionId(id)
        // 基于拿到的SQLBaseData数据创建SQLDataBuilder 调用def this(data: SQLBaseData)
        .map(new SQLDataBuilder(_))
        // id没有信息的情况
        .getOrElse(new SQLDataBuilder(id)))
  }

}
/**
 * SQL 执行开始事件
 *
 * @param queryId       the uuid
 * @param originalSql   the sql
 * @param qe            the query execution
 * @param username      the username of query
 * @param clientAddress the client address of query
 */
@DeveloperApi
case class SQLExecutionStart private() extends SparkListenerEvent {
  @JsonIgnore var queryId: String = _
  @JsonIgnore var originalSql: String = _
  @JsonIgnore var queryExecution: QueryExecution = _
  @JsonIgnore var username: String = _
  @JsonIgnore var clientAddress: String = _
}

object SQLExecutionStart {
  def apply(queryId: String,
            orginalSql: String,
            queryExecution: QueryExecution,
            username: String,
            clientAddress: String): SQLExecutionStart = {
    val sqlExecutionStart = new SQLExecutionStart()
    sqlExecutionStart.queryId = queryId
    sqlExecutionStart.originalSql = orginalSql
    sqlExecutionStart.queryExecution = queryExecution
    sqlExecutionStart.username = username
    sqlExecutionStart.clientAddress = clientAddress
    sqlExecutionStart
  }

  /**
   * SQL 和 Execution 的关系
   */
  private class SQLRelation {
    var queryId: String = _
    var associatedExecutionIds: Set[Long] = Set[Long]()
    var sql: String = _
    var plan: String = _
    var username: String = _
    var clientAddress: String = _

    def this(queryId: String) {
      this()
      this.queryId = queryId
    }

    def this(queryId: String, plan: String) {
      this(queryId)
      this.plan = plan
    }
  }
}
/**
 * SQL 和 Execution 的关系
 */
private class SQLRelation {
  var queryId: String = _
  var associatedExecutionIds: Set[Long] = Set[Long]()
  var sql: String = _
  var plan: String = _
  var username: String = _
  var clientAddress: String = _

  def this(queryId: String) {
    this()
    this.queryId = queryId
  }

  def this(queryId: String, plan: String) {
    this(queryId)
    this.plan = plan
  }
}