/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.spark.testsuite.detailquery

import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.{CarbonSession, Row, SaveMode}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.spark.util.DataGenerator

/**
 * Test Suite for search mode
 */
class SearchModeTestCase extends QueryTest with BeforeAndAfterAll {

  val numRows = 500 * 1000
  override def beforeAll = {
    sqlContext.sparkSession.asInstanceOf[CarbonSession].startSearchMode()
    sql("DROP TABLE IF EXISTS main")

    val df = DataGenerator.generateDataFrame(sqlContext.sparkSession, numRows)
    df.write
      .format("carbondata")
      .option("tableName", "main")
      .option("table_blocksize", "5")
      .mode(SaveMode.Overwrite)
      .save()
  }

  override def afterAll = {
    sql("DROP TABLE IF EXISTS main")
    sqlContext.sparkSession.asInstanceOf[CarbonSession].stopSearchMode()
  }

  private def sparkSql(sql: String): Seq[Row] = {
    sqlContext.sparkSession.asInstanceOf[CarbonSession].sparkSql(sql).collect()
  }

  private def checkSearchAnswer(query: String) = {
    checkAnswer(sql(query), sparkSql(query))
  }

  test("equal filter") {
    checkSearchAnswer("select id from main where id = '100'")
    checkSearchAnswer("select id from main where planet = 'planet100'")
  }

  test("greater and less than filter") {
    checkSearchAnswer("select id from main where m2 < 4")
  }

  test("IN filter") {
    checkSearchAnswer("select id from main where id IN ('40', '50', '60')")
  }

  test("expression filter") {
    checkSearchAnswer("select id from main where length(id) < 2")
  }

  test("aggregate query") {
    checkSearchAnswer("select city, sum(m1) from main where m2 < 10 group by city")
  }

  test("equal filter and limit") {
    checkSearchAnswer("select id from main where city = 'city1' limit 3")
  }

  test("aggregate query with datamap and fallback to SparkSQL") {
    sql("create datamap preagg on table main using 'preaggregate' as select city, count(*) from main group by city ")
    checkSearchAnswer("select city, count(*) from main group by city")
  }

}