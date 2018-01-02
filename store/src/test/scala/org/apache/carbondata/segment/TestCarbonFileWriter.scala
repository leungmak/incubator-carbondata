package org.apache.carbondata.segment

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.metadata.datatype.{DataTypes, StructField}
import org.apache.carbondata.segment.api.{CarbonFileWriter, SchemaBuilder, TableBuilder}

class TestCarbonFileWriter extends QueryTest with BeforeAndAfterAll {

  test("test write carbon table and read as external table") {
    sql("DROP TABLE IF EXISTS source")

    val tablePath = "./db1/tc1"
    cleanTestTable(tablePath)
    createTestTable(tablePath)

    sql(s"CREATE EXTERNAL TABLE source STORED BY 'carbondata' LOCATION '$tablePath'")
    checkAnswer(sql("SELECT count(*) from source"), Row(1000))

    sql("DROP TABLE IF EXISTS source")
  }

  test("test write carbon table and read by refresh table") {
    sql("DROP DATABASE IF EXISTS db1 CASCADE")

    val tablePath = "./db1/tc1"
    cleanTestTable(tablePath)
    createTestTable(tablePath)

    sql("CREATE DATABASE db1 LOCATION './db1'")
    sql("REFRESH TABLE db1.tc1")
    checkAnswer(sql("SELECT count(*) from db1.tc1"), Row(1000))

    sql("DROP DATABASE IF EXISTS db1 CASCADE")
  }

  private def cleanTestTable(tablePath: String) = {
    if (new File(tablePath).exists()) {
      new File(tablePath).delete()
    }
  }

  private def createTestTable(tablePath: String): Unit = {
    val schema = SchemaBuilder.newInstance
      .addColumn(new StructField("name", DataTypes.STRING), true)
      .addColumn(new StructField("age", DataTypes.INT), false)
      .addColumn(new StructField("height", DataTypes.DOUBLE), false)
      .create

    val table = TableBuilder.newInstance
      .tableName("t1")
      .tablePath(tablePath)
      .tableSchema(schema)
      .create

    assert(table != null)

    val segment = table.openNewSegment()

    val data = Array[String]("amy", "1", "2.3")
    val writer = CarbonFileWriter.newInstance(segment)
    (1 to 1000).foreach { _ => writer.writeRow(data) }
    Thread.sleep(100)
    writer.close()

    table.commitSegment(segment)
  }

}
