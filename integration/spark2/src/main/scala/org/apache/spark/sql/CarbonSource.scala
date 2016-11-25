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

package org.apache.spark.sql

import org.apache.carbondata.core.constants.CarbonCommonConstants

import scala.language.implicitConversions
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.execution.CarbonLateDecodeStrategy
import org.apache.spark.sql.optimizer.CarbonLateDecodeRule
import org.apache.carbondata.spark.CarbonOption
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.execution.command.{CreateTable, Field}

/**
 * Carbon relation provider compliant to data source api.
 * Creates carbon relations
 */
class CarbonSource extends RelationProvider
    with CreatableRelationProvider with SchemaRelationProvider with DataSourceRegister {

  override def shortName(): String = "carbondata"

  /**
   * Returns a new base relation with the given parameters.
   * Note: the parameters' keywords are case insensitive and this insensitivity is enforced
   * by the Map that is passed to the function.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    // if path is provided we can directly create Hadoop relation. \
    // Otherwise create datasource relation
    CarbonEnv.init(sqlContext)
    addLateCodeOptimization(sqlContext.sparkSession)
    parameters.get("path") match {
      case Some(path) => CarbonDatasourceHadoopRelation(sqlContext.sparkSession, Array(path), parameters, None)
      case _ => CarbonDatasourceHadoopRelation(sqlContext.sparkSession, Array.empty[String], parameters, None)
    }
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    CarbonEnv.init(sqlContext)
    // User should not specify path since only one store is supported in carbon currently,
    // after we support multi-store, we can remove this limitation
    require(!parameters.contains("path"), "'path' should not be specified, " +
        "the path to store carbon file is the 'storePath' specified when creating CarbonContext")

    val options = new CarbonOption(parameters)
    val storePath = sqlContext.sparkSession.conf.get(CarbonCommonConstants.STORE_LOCATION)
    val tablePath = new Path(storePath + "/" + options.dbName + "/" + options.tableName)
    val isExists = tablePath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
        .exists(tablePath)
    val (doSave, doAppend) = (mode, isExists) match {
      case (SaveMode.ErrorIfExists, true) =>
        sys.error(s"ErrorIfExists mode, path $storePath already exists.")
      case (SaveMode.Overwrite, true) =>
        sqlContext.sparkSession.sql(s"DROP TABLE IF EXISTS ${ options.dbName }.${ options.tableName }")
        (true, false)
      case (SaveMode.Overwrite, false) | (SaveMode.ErrorIfExists, false) =>
        (true, false)
      case (SaveMode.Append, _) =>
        (false, true)
      case (SaveMode.Ignore, exists) =>
        (!exists, false)
    }

    if (doSave) {
      // save data when the save mode is Overwrite.
      new CarbonDataFrameWriter(sqlContext, data).saveAsCarbonFile(parameters)
    } else if (doAppend) {
      new CarbonDataFrameWriter(sqlContext, data).appendToCarbonFile(parameters)
    }

    createRelation(sqlContext, parameters)
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      dataSchema: StructType): BaseRelation = {
    CarbonEnv.init(sqlContext)
    addLateCodeOptimization(sqlContext.sparkSession)
    createTableIfNotExists(sqlContext.sparkSession, parameters, dataSchema)
    parameters.get("path") match {
      case Some(path) =>
        CarbonDatasourceHadoopRelation(sqlContext.sparkSession, Array(path), parameters, Option(dataSchema))
      case _ =>
        CarbonDatasourceHadoopRelation(sqlContext.sparkSession, Array.empty[String], parameters, Option(dataSchema))
    }
  }

  private def addLateCodeOptimization(ss: SparkSession): Unit = {
    ss.sessionState.experimentalMethods.extraStrategies = Seq(new CarbonLateDecodeStrategy)
    ss.sessionState.experimentalMethods.extraOptimizations = Seq(new CarbonLateDecodeRule)
  }

  private def createTableIfNotExists(sparkSession: SparkSession, parameters: Map[String, String],
                                     dataSchema: StructType):
  Unit
  = {
    val options = new CarbonOption(parameters)
    try {
      CarbonEnv.get.carbonMetastore.lookupRelation(Option(options.dbName), options.tableName)
    } catch {
      case ex: NoSuchTableException =>
        val fields = dataSchema.map { col =>
          val column = col.name
          val dataType = Option(col.dataType.toString)
          val name = Option(col.name)
          // This is to parse complex data types
          val x = col.name + ' ' + col.dataType
          val f: Field = Field(column, dataType, name, None, null)
          // the data type of the decimal type will be like decimal(10,0)
          // so checking the start of the string and taking the precision and scale.
          // resetting the data type with decimal
          if (f.dataType.getOrElse("").startsWith("decimal")) {
            val (precision, scale) = TableCreator.getScaleAndPrecision(col.dataType.toString)
            f.precision = precision
            f.scale = scale
            f.dataType = Some("decimal")
          }
          f
        }
        val map = scala.collection.mutable.Map[String, String]();
        parameters.foreach { x => map.put(x._1, x._2) }
        val cm = TableCreator.prepareTableModel(false, Option(options.dbName), options.tableName,
          fields, Nil, map)
        CreateTable(cm).run(sparkSession)

      case _ =>
    }
  }

}
