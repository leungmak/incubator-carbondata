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

import java.io.File

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.hdfs.HDFSFileSource
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.hadoop.{CarbonInputFormat, CarbonProjection}

// Write carbondata file by spark and read it by flink
// scalastyle:off println
object BeamExample {
  var spark: SparkSession = _

  def main(args: Array[String]): Unit = {
    // write carbondata file by spark
    val path = writeCarbonFile("carbon_table")

    // read two columns by beam
    val conf = new Configuration()
    val projection = new CarbonProjection
    projection.addColumn("c1")  // column c1
    projection.addColumn("c3")  // column c3
    CarbonInputFormat.setColumnProjection(conf, projection)

    val p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).create())
    val records = HDFSFileSource.readFrom(
      path,
      classOf[CarbonInputFormat[Array[Object]]],
      classOf[Void],
      classOf[Array[Object]]
    )

    p.run().waitUntilFinish()
    println(records)

    // delete carbondata file
    cleanCarbonFile("carbon_table")
  }

  private def cleanCarbonFile(tableName: String): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
  }

  private def writeCarbonFile(tableName: String): String = {
    val rootPath = new File(this.getClass.getResource("/").getPath
        + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target/metastore_db"

    CarbonProperties.getInstance()
        .addProperty("carbon.kettle.home", s"$rootPath/processing/carbonplugins")
        .addProperty("carbon.storelocation", storeLocation)
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    import org.apache.spark.sql.CarbonSession._

    spark = SparkSession
        .builder()
        .master("local")
        .appName("CarbonExample")
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", warehouse)
        .config("javax.jdo.option.ConnectionURL",
          s"jdbc:derby:;databaseName=$metastoredb;create=true")
        .getOrCreateCarbonSession()

    spark.sparkContext.setLogLevel("WARN")

    spark.sql("DROP TABLE IF EXISTS carbon_table")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE $tableName(
         |    shortField short,
         |    intField int,
         |    bigintField long,
         |    doubleField double,
         |    stringField string,
         |    timestampField timestamp,
         |    decimalField decimal(18,2),
         |    dateField date,
         |    charField char(5)
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    val path = s"$rootPath/examples/spark2/src/main/resources/data.csv"

    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE $tableName
         | options('FILEHEADER'='shortField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField')
       """.stripMargin)

    storeLocation + "/default/" + tableName
  }
}
// scalastyle:on println
