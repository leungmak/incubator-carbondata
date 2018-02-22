/*
 * Copyright (c) Huawei Futurewei Technologies, Inc. All Rights Reserved.
 *
 */

package org.apache.carbondata.mv.plans

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter

import org.apache.carbondata.mv.dsl.plans._
import scala.util.{Failure, Success, Try}

class Tpcds_1_4_BenchmarkSuite extends PlanTest with BeforeAndAfter {
  import org.apache.carbondata.mv.Tpcds_1_4_QueryBatch._
  import org.apache.carbondata.mv.Tpcds_1_4_Tables._

  val spark = SparkSession.builder().master("local").enableHiveSupport().getOrCreate()
  // spark.conf.set("spark.sql.crossJoin.enabled", true)
  val testHive = new org.apache.spark.sql.hive.test.TestHiveContext(spark.sparkContext, false)
  val hiveClient = testHive.sparkSession.metadataHive

  test("test SQLBuilder using tpc-ds queries") {

    tpcds1_4Tables.foreach { create_table =>
      hiveClient.runSqlHive(create_table)
    }

//    val dest = "qTradeflow"  // this line is for development, comment it out once done
    val dest = "qSEQ"
//    val dest = "qAggPushDown"    // this line is for development, comment it out once done
//    val dest = "q10"

    tpcds1_4Queries.foreach { query =>
      if (query._1 == dest) {  // this line is for development, comment it out once done
        val analyzed = testHive.sql(query._2).queryExecution.analyzed
        logInfo(s"""\n\n===== Analyzed Logical Plan for ${query._1} =====\n\n$analyzed \n""")
        
//        val cnonicalizedPlan = new SQLBuilder(analyzed).Canonicalizer.execute(analyzed)
//        
//        Try(new SQLBuilder(analyzed).toSQL) match {
//          case Success(s) => logInfo(s"""\n\n===== CONVERTED back ${query._1} USING SQLBuilder =====\n\n$s \n""")
//          case Failure(e) => logInfo(s"""Cannot convert the logical query plan of ${query._1} back to SQL""")
//        }
        
        // this Try is for development, comment it out once done
        Try(analyzed.optimize) match {
          case Success(o) => {
            logInfo(s"""\n\n===== Optimized Logical Plan for ${query._1} =====\n\n$o \n""")
          }
          case Failure(e) =>
        }

        val o = analyzed.optimize
        val o1 = o.modularize
        
        Try(o.modularize.harmonize) match {
          case Success(m) => {
            logInfo(s"""\n\n===== MODULAR PLAN for ${query._1} =====\n\n$m \n""")

            Try(m.asCompactSQL) match {
              case Success(s) => logInfo(s"\n\n===== CONVERTED SQL for ${query._1} =====\n\n${s}\n")
              case Failure(e) => logInfo(s"""\n\n===== CONVERTED SQL for ${query._1} failed =====\n\n${e.toString}""")
            }
          }
          case Failure(e) => logInfo(s"""\n\n==== MODULARIZE the logical query plan for ${query._1} failed =====\n\n${e.toString}""")
        }
      }
    }

  }
}