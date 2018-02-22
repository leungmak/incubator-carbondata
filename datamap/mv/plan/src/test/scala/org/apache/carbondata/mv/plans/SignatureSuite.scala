/*
 * Copyright (c) Huawei Futurewei Technologies, Inc. All Rights Reserved.
 *
 */

package org.apache.carbondata.mv.plans

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util._
import org.scalatest.BeforeAndAfter

import org.apache.carbondata.mv.Tpcds_1_4_Tables.tpcds1_4Tables
import org.apache.carbondata.mv.dsl.plans._
import org.apache.carbondata.mv.plans.modular.ModularPlanSignatureGenerator

class SignatureSuite extends PlanTest with BeforeAndAfter {
  import org.apache.carbondata.mv.TestSQLBatch._
  
  val spark = SparkSession.builder().master("local").enableHiveSupport().getOrCreate()
  val testHive = new org.apache.spark.sql.hive.test.TestHiveContext(spark.sparkContext, false)
  val hiveClient = testHive.sparkSession.metadataHive
  
  test("test signature computing") {

      tpcds1_4Tables.foreach { create_table =>
        hiveClient.runSqlHive(create_table)
      }

    hiveClient.runSqlHive(
        s"""
           |CREATE TABLE Fact (
           |  `A` int,
           |  `B` int,
           |  `C` int,
           |  `E` int,
           |  `K` int
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
           |STORED AS TEXTFILE
        """.stripMargin.trim
        )

    hiveClient.runSqlHive(
        s"""
           |CREATE TABLE Dim (
           |  `D` int,
           |  `E` int,
           |  `K` int
           |)
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
           |STORED AS TEXTFILE
        """.stripMargin.trim
        )

    hiveClient.runSqlHive(
      s"""
         |CREATE TABLE Dim1 (
         |  `F` int,
         |  `G` int,
         |  `K` int
         |)
         |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
         |STORED AS TEXTFILE
        """.stripMargin.trim
    )

    testSQLBatch.foreach { query =>
      val analyzed = testHive.sql(query).queryExecution.analyzed
      val modularPlan = analyzed.optimize.modularize
      val sig = ModularPlanSignatureGenerator.generate(modularPlan)
      sig match {
        case Some(s) if (s.groupby != true || s.datasets != Set("default.fact","default.dim")) =>
          logError(
              s"""
              |=== FAIL: signature do not match ===
              |${sideBySide(s.groupby.toString, true.toString).mkString("\n")}
              |${sideBySide(s.datasets.toString, Set("Fact","Dim").toString).mkString("\n")}
            """.stripMargin)
        case _ =>
      }
    }
  }
  testHive.sparkSession.cloneSession()
}