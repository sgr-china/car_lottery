package org.apache.spark.sql.execution.ui.customui

import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.execution.ui.{SQLCustomizeData, SQLCustomizeStore}
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.json4s.JsonAST

import javax.servlet.http.HttpServletRequest
import scala.xml.{Node, NodeBuffer}

/**
 * @author sunguorui
 * @date 2023年05月24日 6:08 下午
 */

class CustomizeDescPage(parent: CustomizeTab, store: SQLCustomizeStore) extends WebUIPage("Customize") {
  private val tableHeader = <thead>
    <tr>
      <th>
        <span data-toggle="tooltip" data-placement="top" title="" data-original-title="">
          AttributeName
          &nbsp;
          ▾
        </span>
      </th>
      <th>
        <span data-toggle="tooltip" data-placement="top" title="" data-original-title="">
          Details
        </span>
      </th>
    </tr>
  </thead>

  override def render(request: HttpServletRequest): Seq[Node] = {
    val sqlId = request.getParameter("id")
    require(sqlId != null && sqlId.nonEmpty, "Missing sql id parameter")

    val maybe = store.getCustomizeData(sqlId)

    val table =
      maybe.map { d =>
        <table class="table table-bordered table-sm table-striped table-head-clickable table-cell-width-limited">
          {tableHeader}<tbody>
          {customizeDesc(d)}
        </tbody>
        </table>
      }
    val content = table.getOrElse(<h4>Not found sql for
      {sqlId}
      .</h4>)

    UIUtils.headerSparkPage(
      request, s"Audit Details for SQL '$sqlId'.", content, parent)
  }

  private def formatIntervalTime(duration: Long): String = {
    if (duration > 1000 * 60) {
      s"${duration / 1000 * 60} min"
    } else if (duration > 1000) {
      s"${duration / 1000} s"
    } else {
      s"$duration ms"
    }
  }

  private def customizeDesc(d: SQLCustomizeData): NodeBuffer = {
    <tr>
      <td>
        QUERY ID
      </td>
      <td>
        {d.queryId}
      </td>
    </tr>

      <tr>
        <td>
          EXECUTION ID
        </td>
        <td>
          {d.executionId}
        </td>
      </tr>

      <tr>
        <td>
          ASSOCIATED EXECUTION IDS
        </td>
        <td>
          {d.associatedExecutionIds.mkString(",")}
        </td>
      </tr>

      <tr>
        <td>
          APPLICATION NAME
        </td>
        <td>
          {d.applicationName}
        </td>
      </tr>

      <tr>
        <td>
          USERNAME
        </td>
        <td>
          {d.username}
        </td>
      </tr>

      <tr>
        <td>
          STATUS
        </td>
        <td>
          {if (d.status) "SUCCESS" else "FAILED"}
        </td>
      </tr>

      <tr>
        <td>
          MESSAGE
        </td>
        <td>
          { if (null != d.message) {
              <pre>{d.message}</pre>
            }
          }
        </td>
      </tr>

      <tr>
        <td>
          QUERY START
        </td>
        <td>
          {d.queryStart}
        </td>
      </tr>

      <tr>
        <td>
          DURATION
        </td>
        <td>
          {formatIntervalTime(d.duration)}
        </td>
      </tr>

      <tr>
        <td>
          ENGINE TYPE
        </td>
        <td>
          {d.engineType}
        </td>
      </tr>

      <tr>
        <td>
          CLIENT ADDRESS
        </td>
        <td>
          {d.clientAddress}
        </td>
      </tr>

      <tr>
        <td>
          ORIGINAL SQL
        </td>
        <td>
          <pre>{d.originalSql}</pre>
        </td>
      </tr>

      <tr>
        <td>
          PARSED LOGICAL PLAN
        </td>
        <td>
          <pre>{d.parsedLogicalPlan}</pre>
        </td>
      </tr>

      <tr>
        <td>
          OPTIMIZED LOGICAL PLAN
        </td>
        <td>
          <pre>{d.optimizedLogicalPlan}</pre>
        </td>
      </tr>

      <tr>
        <td>
          WRITTEN TABLES
        </td>
        <td>
          {JsonUtils.toJson(d.writtenTables)}
        </td>
      </tr>

      <tr>
        <td>
          READ TABLES
        </td>
        <td>
          {JsonUtils.toJson(d.readTables)}
        </td>
      </tr>

      <tr>
        <td>
          COMMAND TAG
        </td>
        <td>
          {d.commandTag}
        </td>
      </tr>

      <tr>
        <td>
          PHYSICAL PLAN
        </td>
        <td>
          <pre>{d.physicalPlan}</pre>
        </td>
      </tr>

      <tr>
        <td>
          PHYSICAL READS
        </td>
        <td>
          {d.physicalReads}
        </td>
      </tr>

      <tr>
        <td>
          SCAN ROWS
        </td>
        <td>
          {d.scanRows}
        </td>
      </tr>

      <tr>
        <td>
          SCAN BYTES
        </td>
        <td>
          {d.scanBytes}
        </td>
      </tr>

      <tr>
        <td>
          RESULT ROWS
        </td>
        <td>
          {d.resultRows}
        </td>
      </tr>

      <tr>
        <td>
          RESULT BYTES
        </td>
        <td>
          {d.resultBytes}
        </td>
      </tr>

      <tr>
        <td>
          AFFECTED ROWS
        </td>
        <td>
          {d.affectedRows}
        </td>
      </tr>

      <tr>
        <td>
          AFFECTED BYTES
        </td>
        <td>
          {d.affectedBytes}
        </td>
      </tr>

      <tr>
        <td>
          QUERY START COST
        </td>
        <td>
          {d.queryStartCost}
        </td>
      </tr>

      <tr>
        <td>
          EXECUTION COST
        </td>
        <td>
          {d.executionCost}
        </td>
      </tr>

      <tr>
        <td>
          EXTENDED COST
        </td>
        <td>
          {d.extendedCost}
        </td>
      </tr>

      <tr>
        <td>
          STATISTICS
        </td>
        <td>
          { if (null != d.statistics) {
              <pre>{d.statistics}</pre>
            }
          }
        </td>
      </tr>

      <tr>
        <td>
          MEMORY BYTES
        </td>
        <td>
          {d.memoryBytes}
        </td>
      </tr>

      <tr>
        <td>
          SHUFFLE BYTES
        </td>
        <td>
          {d.shuffleBytes}
        </td>
      </tr>

      <tr>
        <td>
          CPU TIME MS
        </td>
        <td>
          {d.cpuTimeMs}
        </td>
      </tr>
  }
}
