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

package org.apache.carbondata.examples

import org.apache.carbondata.examples.util.InitForExamples
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.carbondata.spark._

object DatasourceExample {

  def main(args: Array[String]) {
    val cc = InitForExamples.createCarbonContext("DatasourceExample")

    import cc.implicits._

    val sc = cc.sparkContext
    // create a dataframe, it can be from parquet or hive table
    val df = sc.parallelize(1 to 1000)
               .map(x => ("a", "b", x))
               .toDF("c1", "c2", "c3")

    // save dataframe to carbon file
    df.write
      .format("carbondata")
      .option("tableName", "carbon1")
      .mode(SaveMode.Overwrite)
      .save()

    val sqlContext = new SQLContext(sc)

    sqlContext.sql(
      """
        | CREATE TEMPORARY TABLE source
        | (id int, name string, city string)
        | USING org.apache.spark.sql.CarbonSource
        | OPTIONS (path './examples/target/store/default/carbon1')
      """.stripMargin)

    sqlContext.sql("SELECT count(*) FROM source").show
    sqlContext.sql("DROP TABLE IF EXISTS source")
  }
}
