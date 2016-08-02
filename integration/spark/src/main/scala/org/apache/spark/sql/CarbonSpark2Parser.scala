package org.apache.spark.sql

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class CarbonSpark2Parser extends ParserInterface {

  lazy val parser = new CarbonSqlParser

  /** Creates LogicalPlan for a given SQL string. */
  def parsePlan(sqlText: String): LogicalPlan = {
    parser.parse(sqlText)
  }

  /** Creates Expression for a given SQL string. */
  def parseExpression(sqlText: String): Expression = {
    null
  }

  /** Creates TableIdentifier for a given SQL string. */
  def parseTableIdentifier(sqlText: String): TableIdentifier = {
    null
  }
}
