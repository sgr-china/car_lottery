package org.apache.spark.sql.execution.ui.customui

import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.execution.ui.{SQLBaseData, SQLCustomizeStore}
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.json4s.JsonAST

import javax.servlet.http.HttpServletRequest
import scala.xml.{Elem, Node}

/**
 * @author sunguorui
 * @date 2023年05月24日 6:08 下午
 */

class CustomizePage(parent: CustomizeTab, store: SQLCustomizeStore) extends WebUIPage("") {
  private val tableHeader = <thead>
    <tr>
      <th>
        <span data-toggle="tooltip" data-placement="top" title="" data-original-title="">
          ID
          &nbsp;
          ▾
        </span>
      </th>
      <th>
        <span data-toggle="tooltip" data-placement="top" title="" data-original-title="">
          SQL
        </span>
      </th>
      <th>
        <span data-toggle="tooltip" data-placement="top" title="" data-original-title="">
          Submitted
        </span>
      </th>
      <th>
        <span data-toggle="tooltip" data-placement="top" title="" data-original-title="">
          Duration
        </span>
      </th>
      <th>
        <span data-toggle="tooltip" data-placement="top" title="" data-original-title="">
          Execution IDs
        </span>
      </th>
    </tr>
  </thead>
  override def render(request: HttpServletRequest): Seq[Node] = {

    val content =
      <table class="table table-bordered table-sm table-striped table-head-clickable table-cell-width-limited">
        {tableHeader}<tbody>
        {store.customizeBaseDataList().map(customizeTableLine(request, _))}
      </tbody>
      </table>


    UIUtils.headerSparkPage(
      request, s"Audits for SQL.", content, parent)
  }

  private def customizeTableLine(request: HttpServletRequest, d: SQLBaseData): Elem = {
    val sqlUrl = "%s/Audits/audit/?id=%s".format(UIUtils.prependBaseUri(request, parent.basePath), d.queryId)

    <tr>
      <td>
        <a href={sqlUrl}>
          {d.queryId}
        </a>
      </td>
      <td>
        <pre>
          {d.originalSql}
        </pre>
      </td>
      <td>
        {d.queryStart}
      </td>
      <td>
        {d.duration}
      </td>
      <td>
        {d.executionId}
        [
        {d.associatedExecutionIds}
        ]</td>
    </tr>
  }

  override def renderJson(request: HttpServletRequest): JsonAST.JValue = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._

    val json = JsonUtils.toJson(store.collectCustomizeData())
    parse(json)
  }
}
