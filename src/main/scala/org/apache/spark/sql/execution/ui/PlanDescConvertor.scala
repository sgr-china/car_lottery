package org.apache.spark.sql.execution.ui

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.delta.commands.UpdateCommand
import org.apache.spark.sql.execution.command.{DropTableCommand, ExplainCommand}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.util.Try
/**
 * @author sunguorui
 * @date 2023年05月30日 6:33 下午
 */
object PlanDescConvertor {

  def convertPlanDesc(plan: LogicalPlan, result: LogicalPlanDesc, isRead: Boolean = true): Unit = {
    plan match {
      // Query Task
      case ds: DataSourceV2Relation =>
        val nameSpace = ds.identifier.map(i => name(i.namespace())).orNull
        setCommandTag(result, SQLCommandTags.Select())
        // 区分是任务读的表还是写的表
        parserTable(ds.table).foreach(t => {
          val info = TableInfo(t.name,
            t.database,
            nameSpace,
            t.datasourceType.map(_.name).orNull)
          if (!isRead) {
            result.writtenTables += info
          } else {
            result.readTables += info
          }
        })
      case t: ResolvedTable =>
        val nameSpace = name(t.identifier.namespace())
        // 区分是任务读的表还是写的表
        parserTable(t.table).foreach(t => {
          val info = TableInfo(t.name,
            t.database,
            nameSpace,
            t.datasourceType.map(_.name).orNull)
          if (!isRead) {
            result.writtenTables += info
          } else {
            result.readTables += info
          }
        })
      // Create Table Task
      case ct: CreateV2Table =>
        result.commandTag = SQLCommandTags.CreateTable()
        val table = ct.tableName
        val catalogTable = ct.catalog.loadTable(table)
        parserTable(catalogTable).foreach(t => {
          result.writtenTables += TableInfo(table.name(),
            t.database,
            name(table.namespace()),
            t.datasourceType.map(_.name).orNull)
        })

      // Insert Table Task
      case append: AppendData =>
        result.commandTag = SQLCommandTags.Insert()
        convertPlanDesc(append.table, result, false)

      // Explain Task
      case e: ExplainCommand =>
        result.commandTag = SQLCommandTags.Explain()
        convertPlanDesc(e.logicalPlan, result)

      // Update Task
      case t: UpdateCommand =>
        result.commandTag = SQLCommandTags.Update()
        convertPlanDesc(t.target, result, false)

      // Delete Task
      case t: DeleteFromTable =>
        result.commandTag = SQLCommandTags.Delete()
        convertPlanDesc(t.table, result, false)

      // Drop Table Task
      case t: DropTableCommand =>
        result.commandTag = SQLCommandTags.DropTable()
        result.writtenTables += TableInfo(t.tableName.table,
          t.tableName.database.orNull, t.tableName.identifier, null)
      case t: NoopCommand =>
        result.writtenTables += TableInfo(
          Try(t.multipartIdentifier(1)) getOrElse null,
          Try(t.multipartIdentifier.head) getOrElse null, null, null)

      // Alter Table Task
      case t: AlterTableCommand =>
        result.commandTag = SQLCommandTags.AlterTable()
        convertPlanDesc(t.table, result, false)
      case t: RenameTable =>
        result.commandTag = SQLCommandTags.AlterTable()
        convertPlanDesc(t.child, result, false)


      case _ =>
    }
    plan.children.foreach(p => convertPlanDesc(p, result, isRead))
  }


  case class TableDesc(name: String, database: String, datasourceType: Option[DataSourceType])
  private def parserTable(table: Table): Option[TableDesc] = {
    table match {
//      case t: LakeHouseJDBCTable =>
//        val url = t.jdbcOptions().url
//        Some(TableDesc(table.name(), parserDatabaseByJdbcUrl(url),
//          parserSourceTypeByJdbcUrl(url)))
//      case t: MongoTable =>
//        val config = MongoUtils.getReadConfig(new CaseInsensitiveStringMap(t.properties()))
//        Some(TableDesc(t.name(), config.databaseName, Some(DataSourceTypes.Mongo())))
//      case t: ElasticsearchTable =>
//        Some(TableDesc(t.name(), null /*todo*/ , Some(DataSourceTypes.ElasticSearch())))
      case _ => None // todo add more table
    }
  }

  /**
   * catalog中传入的 namespace都是 namespace: Array[String] 这种类型的<br/>
   * 这是为了支持多级 namespace，比如 ["lakehouse","default"]，就是 二级的namespace<br/>
   * 这里 将其拼接为 "lakehouse.default" 将层次打平
   *
   * @param names
   * @return
   */
  def name(names: Array[String]): String = {
    // 这里理论上不应该会出现空，出现空时语法判断就会出错
    if (names == null || names.length == 0) {
      throw new Exception("names is emtpy")
    }
    // 将 [hello,aa,bb,cc] 拼接为 hello.aa.bb.cc 这种格式
    // 最后一个 cc 后面不需要加 . 所以循环遍历的次数为 names.length-2(正常遍历所有的是names.length-1)
    // 如果names.length为 1，则执行执行 循环后面的那个 sb.append
    val sb: StringBuilder = new StringBuilder()
    for (i <- 0 to names.length - 2) {
      sb.append(names(i)).append(".")
    }
    sb.append(names(names.length - 1))
    sb.toString()
  }

  private def setCommandTag(result: LogicalPlanDesc, tag: SQLCommandTag): Unit = {
    if (result.commandTag == null) {
      result.commandTag = tag
    }
  }

  private def parserSourceTypeByJdbcUrl(url: String ): Option[DataSourceType] = {
    if (StringUtils.startsWithIgnoreCase(url, "jdbc:mysql:")) {
      Some(DataSourceTypes.MySql())
    } else if (StringUtils.startsWithIgnoreCase(url, "jdbc:postgresql")) {
      Some(DataSourceTypes.PostgreSql())
    } else if (StringUtils.startsWithIgnoreCase(url, "jdbc:mssql")) {
      Some(DataSourceTypes.MSSql())
    } else if (StringUtils.startsWithIgnoreCase(url, "jdbc:db2")) {
      Some(DataSourceTypes.DB2())
    } else if (StringUtils.startsWithIgnoreCase(url, "jdbc:mariadb")) {
      Some(DataSourceTypes.Mariadb())
    } else if (StringUtils.startsWithIgnoreCase(url, "jdbc:impala")) {
      Some(DataSourceTypes.Impala())
    } else if (StringUtils.startsWithIgnoreCase(url, "jdbc:hive2")) {
      Some(DataSourceTypes.Hive())
    } else if (StringUtils.startsWithIgnoreCase(url, "jdbc:oracle")) {
      Some(DataSourceTypes.Oracle())
    } else {
      // todo Add more jdbc type
      None
    }
  }

  private lazy val DATABASE_NAME_REGEX = "jdbc:.*//.*/([a-zA-Z\\d_-]*).*".r

  private def parserDatabaseByJdbcUrl(url: String): String = {
    DATABASE_NAME_REGEX.findFirstMatchIn(url).map(_.group(1)).orNull
  }


}
