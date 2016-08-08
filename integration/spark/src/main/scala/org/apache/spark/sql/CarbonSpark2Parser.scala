package org.apache.spark.sql

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.CreateTableContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{CreateTable, CreateTableCommand, PartitionerField, tableModel}
import org.apache.spark.sql.execution.{SparkSqlAstBuilder, SparkSqlParser}
import org.apache.spark.sql.internal.SQLConf

class CarbonSpark2Parser(conf: SQLConf) extends SparkSqlParser(conf) {

  override val astBuilder = new CarbonSqlAstBuilder(conf)

  val parser = new CarbonSqlParser


  override def parsePlan(sqlText: String): LogicalPlan = {
    try {
      parser.parse(sqlText.toLowerCase)
    } catch {
      case _ => super.parsePlan(sqlText.toLowerCase)
    }
  }

  class CarbonSqlAstBuilder(conf: SQLConf) extends SparkSqlAstBuilder(conf) {
    override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = {
      val cc = super.visitCreateTable(ctx)
      val create = cc.asInstanceOf[CreateTableCommand]
      val table = create.table
      var partitionCols: Seq[PartitionerField] = Seq[PartitionerField]()
      val model = parser.prepareTableModel(create.ifNotExists, table.identifier.database,
        table.identifier.table, parser.createFields(table.schema), partitionCols, table.properties)
      CreateTable(model)
    }
  }
}
