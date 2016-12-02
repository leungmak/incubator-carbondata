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

package org.apache.spark.sql.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.TableLoader

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object CarbonExample {

  def main(args: Array[String]): Unit = {
    // to run the example, plz change this path to your local machine path
    val rootPath = "/users/jackylk/code/incubator-carbondata"
    val spark = SparkSession
        .builder()
        .master("local")
        .appName("CarbonExample")
        .enableHiveSupport()
        .config(CarbonCommonConstants.STORE_LOCATION,
          s"$rootPath/examples/spark2/target/store")
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // Drop table
    spark.sql("DROP TABLE IF EXISTS carbon_table")
    spark.sql("DROP TABLE IF EXISTS csv_table")
//
//    // Create table
    spark.sql(
      s"""
         | CREATE TABLE carbon_table(
         |    shortField short,
         |    intField int,
         |    bigintField long,
         |    doubleField double,
         |    charField char(10),
         |    stringField string,
         |    timestampField timestamp,
         |    dateField date
         | )
         | USING org.apache.spark.sql.CarbonSource
       """.stripMargin)

    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    val prop = s"$rootPath/conf/dataload.properties.template"
    val tableName = "carbon_table"
    val path = s"$rootPath/examples/spark2/src/main/resources/data.csv"
    TableLoader.main(Array[String](prop, tableName, path))

    spark.sql("""
             SELECT *
             FROM carbon_table
              """).show

//    spark.sql("""
//             SELECT *
//             FROM carbon_table where length(stringField) = 5
//              """).show
//
//    spark.sql("""
//           SELECT sum(intField), stringField
//           FROM carbon_table
//           GROUP BY stringField
//           """).show
//
//    spark.sql(
//      """
//        |select t1.*, t2.*
//        |from carbon_table t1, carbon_table t2
//        |where t1.stringField = t2.stringField
//      """.stripMargin).show

    // Drop table
//    spark.sql("DROP TABLE IF EXISTS carbon_table")
//    spark.sql("DROP TABLE IF EXISTS csv_table")
  }
}
