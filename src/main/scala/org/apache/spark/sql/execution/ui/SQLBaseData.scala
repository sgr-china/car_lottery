package org.apache.spark.sql.execution.ui

import com.fasterxml.jackson.annotation.JsonIgnore
import java.lang.{Boolean => JBoolean}
import org.apache.spark.status.KVUtils.KVIndexParam
import org.apache.spark.util.kvstore.KVIndex

import java.sql.{Date, Timestamp}

/**
 * @author sunguorui
 * @date 2023年05月24日 9:10 下午
 */

class SQLBaseData(@KVIndexParam val queryId: String,
                  val executionId: Long,
                  val associatedExecutionIds: Set[Long],
                  val applicationName: String,
                  val username: String,
                  val status: Boolean,
                  val message: String,
                  val queryStart: Long,
                  val duration: Long,
                  val engineType: String,
                  val clientAddress: String,

                  val originalSql: String,
                  val parsedLogicalPlan: String,
                  val optimizedLogicalPlan: String,

                  val writtenTables: Set[TableInfo],
                  val readTables: Set[TableInfo],

                  val commandTag: String,

                  val physicalPlan: String,
                  val physicalReads: Int) {
  @JsonIgnore
  @KVIndex("completionTime")
  private def completionTimeIndex: Long = queryStart + duration
}

case class TableInfo
(
  name: String,
  databaseName: String,
  nameSpace: String,
  dataSourceType: String
)

/**
 * 审计日志的统一抽象
 * 由于 delta 数据库不支持嵌套字段 (PS: 支持Array) 写入，故此处将所有字段重复定义了一遍
 * 且使用了 样例类 Schema 自动提取功能，故此类必须为样例类
 */
case class SQLCustomizeData
(
  queryId: String,
  executionId: Long,
  associatedExecutionIds: Array[Long],
  applicationName: String,
  username: String,
  status: Boolean,
  message: String,
  queryStart: Timestamp,
  duration: Long,
  engineType: String,
  clientAddress: String,
  // partition key
  currentDay: Date,

  originalSql: String,
  parsedLogicalPlan: String,
  optimizedLogicalPlan: String,

  writtenTables: Array[TableInfo],
  readTables: Array[TableInfo],

  commandTag: String,

  physicalPlan: String,
  physicalReads: Int,

  scanRows: String,
  scanBytes: String,

  resultRows: String,
  resultBytes: String,

  affectedRows: String,
  affectedBytes: String,
  queryStartCost: String,
  executionCost: String,
  extendedCost: String,
  statistics: String,
  memoryBytes: String,
  shuffleBytes: String,
  cpuTimeMs: String,
) {
  def this(baseData: SQLBaseData) {
    this(baseData.queryId, baseData.executionId, baseData.associatedExecutionIds.toArray, baseData.applicationName,
      baseData.username, baseData.status, baseData.message, new Timestamp(baseData.queryStart),
      baseData.duration, baseData.engineType, baseData.clientAddress, new Date(baseData.queryStart),
      baseData.originalSql, baseData.parsedLogicalPlan, baseData.optimizedLogicalPlan,
      baseData.writtenTables.toArray, baseData.readTables.toArray, baseData.commandTag,
      baseData.physicalPlan, baseData.physicalReads,
      null, null, null, null, null, null, null, null, null, null, null, null, null)
  }
}

class SQLDataBuilder {

  var queryId: String = _
  var executionId: Long = _
  var associatedExecutionIds: Set[Long] = Set[Long]()
  var applicationName: String = _
  var userName: String = _
  var status: JBoolean = _
  var message: String = _
  var queryStart: Long = Long.MaxValue
  var queryEnd: Long = Long.MinValue
  var duration: Long = _
  var engineType: String = "Spark"
  var clientAddress: String = _

  var originalSql: String = _

  var parsedLogicalPlan: String = _
  var optimizedLogicalPlan: String = _

  var writtenTables: Set[TableInfo] = Set[TableInfo]()
  var readTables: Set[TableInfo] = Set[TableInfo]()

  private var commandTag: SQLCommandTag = _

  var physicalPlan: String = _
  var physicalReads: Int = _

  var scanRows: String = _
  var scanBytes: String = _

  var resultRows: String = _
  var resultBytes: String = _

  var affectedRows: String = _
  var affectedBytes: String = _
  var queryStartCost: String = _
  var executionCost: String = _
  var extendedCost: String = _
  var statistics: String = _
  var memoryBytes: String = _
  var shuffleBytes: String = _
  var cpuTimeMs: String = _

  var finished: Boolean = false

  def this(id: Long) {
    this()
    this.executionId = id
  }

  def this(sqlId: String) {
    this()
    this.queryId = sqlId
  }

  def this(data: SQLBaseData) {
    this()
    this.queryId = data.queryId
    this.executionId = data.executionId
    this.associatedExecutionIds = data.associatedExecutionIds
    this.userName = data.username
    this.status = data.status
    this.message = data.message
    this.queryStart = data.queryStart
    this.queryEnd = data.queryStart + data.duration
    this.duration = data.duration
    this.engineType = data.engineType
    this.clientAddress = data.clientAddress
    this.originalSql = data.originalSql
    this.parsedLogicalPlan = data.parsedLogicalPlan
    this.optimizedLogicalPlan = data.optimizedLogicalPlan
    this.writtenTables = data.writtenTables
    this.readTables = data.readTables
    this.commandTag = SQLCommandTags.valueOf(data.commandTag)
    this.physicalPlan = data.physicalPlan
    this.physicalReads = data.physicalReads
  }

  def setPlanDesc(infos: LogicalPlanDesc): Unit = {
    this.readTables = infos.readTables
    this.writtenTables = infos.writtenTables
    this.commandTag = infos.commandTag
  }

  def buildBaseData(): SQLBaseData = {
    new SQLBaseData(queryId, executionId, associatedExecutionIds, applicationName, userName, status, message,
      queryStart, duration, engineType, clientAddress,
      originalSql, parsedLogicalPlan, optimizedLogicalPlan,
      writtenTables, readTables, if (null != commandTag) commandTag.name else null,
      physicalPlan, physicalReads)
  }

  def buildAuditData(): SQLCustomizeData = {
    SQLCustomizeData(queryId, executionId, associatedExecutionIds.toArray, applicationName, userName, status, message,
      new Timestamp(queryStart), duration, engineType, clientAddress, new Date(queryStart),
      originalSql, parsedLogicalPlan, optimizedLogicalPlan,
      writtenTables.toArray, readTables.toArray, if (null != commandTag) commandTag.name else null,
      physicalPlan, physicalReads, scanRows, scanBytes, resultRows, resultBytes, affectedRows, affectedBytes,
      queryStartCost, executionCost, extendedCost, statistics, memoryBytes, shuffleBytes, cpuTimeMs)
  }

}

/**
 * 逻辑计划详细信息
 */
class LogicalPlanDesc {
  var writtenTables: Set[TableInfo] = Set[TableInfo]()
  var readTables: Set[TableInfo] = Set[TableInfo]()
  var commandTag: SQLCommandTag = _
}

/**
 * SQL Command TAG
 */
sealed trait SQLCommandTag {
  def name: String
}

object SQLCommandTags {
  case class Insert() extends SQLCommandTag {
    override def name: String = "INSERT"
  }

  case class Select() extends SQLCommandTag {
    override def name: String = "SELECT"
  }

  case class Update() extends SQLCommandTag {
    override def name: String = "UPDATE"
  }

  case class Delete() extends SQLCommandTag {
    override def name: String = "DELETE"
  }

  case class Explain() extends SQLCommandTag {
    override def name: String = "EXPLAIN"
  }

  case class CreateTable() extends SQLCommandTag {
    override def name: String = "CREATE TABLE"
  }

  case class DropTable() extends SQLCommandTag {
    override def name: String = "DROP TABLE"
  }

  case class AlterTable() extends SQLCommandTag {
    override def name: String = "ALTER TABLE"
  }

  case class AlterDatabase() extends SQLCommandTag {
    override def name: String = "ALTER DATABASE"
  }

  def valueOf(name: String): SQLCommandTag = {
    name match {
      case "INSERT" => Insert()
      case "SELECT" => Select()
      case "UPDATE" => Update()
      case "DELETE" => Delete()
      case "EXPLAIN" => Explain()
      case "CREATE TABLE" => CreateTable()
      case "DROP TABLE" => DropTable()
      case "ALTER TABLE" => AlterTable()
      case "ALTER DATABASE" => AlterDatabase()
      case _ => null
    }
  }
}

/**
 * 数据源类型
 */
sealed trait DataSourceType {
  def name: String
}

object DataSourceTypes {

  case class MySql() extends DataSourceType {
    override def name: String = "MYSQL"
  }

  case class PostgreSql() extends DataSourceType {
    override def name: String = "POSTGRESQL"
  }

  case class MSSql() extends DataSourceType {
    override def name: String = "MSSQL"
  }

  case class DB2() extends DataSourceType {
    override def name: String = "DB2"
  }

  case class Mariadb() extends DataSourceType {
    override def name: String = "MARIADB"
  }

  case class Impala() extends DataSourceType {
    override def name: String = "IMPALA"
  }

  case class Hive() extends DataSourceType {
    override def name: String = "HIVE"
  }

  case class Oracle() extends DataSourceType {
    override def name: String = "ORACLE"
  }

  case class Mongo() extends DataSourceType {
    override def name: String = "MONGO"
  }

  case class ElasticSearch() extends DataSourceType {
    override def name: String = "ELASTICSEARCH"
  }
}
