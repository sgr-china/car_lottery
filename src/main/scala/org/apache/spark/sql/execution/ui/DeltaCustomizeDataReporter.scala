package org.apache.spark.sql.execution.ui

import com.sgr.util.LazyLogging
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.execution.ui.SchemaUtils.schema

/**
 * @author sunguorui
 * @date 2023年05月29日 9:23 下午
 */
class DeltaCustomizeDataReporter(val spark: SparkSession) extends CustomizeDataReporter with LazyLogging {

  private val PATH_PREFIX = ""
  private val fileName = "customizeData_log"

  override def invoke(data: Seq[SQLCustomizeData]): Unit = {
    if (data.isEmpty) {
      return
    }
    this.addAll(data)
  }

  private def addAll(data: Seq[SQLCustomizeData]): Unit = {
    /*
    需要更新时的方案
    val map = SchemaUtils.names(schema).map { n => (n, s"n.$n") }.toMap

    deltaTable.as("o")
      .merge(
        nDF.as("n"),
        "o.queryId = n.queryId")
      .whenMatched
      .updateExpr(
        map.filterKeys(_.equals("queryId"))
      )
      .whenNotMatched
      .insertExpr(
        map
      )
      .execute() */

    import io.delta.implicits._

    val df = Dataset.ofRows(spark, LocalRelation.fromProduct(schema[SQLCustomizeData].toAttributes, data))

    df
      .write
      .mode("append")
      .delta(PATH_PREFIX + fileName)

    logger.debug(s"delta insert $data")
  }

}
