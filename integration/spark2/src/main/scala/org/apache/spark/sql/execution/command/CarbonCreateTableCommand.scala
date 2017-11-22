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

package org.apache.spark.sql.execution.command

import java.util
import java.util.List

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.{Row, SparkSession, _}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.types.{StructType => SparkStructType}
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.exception.InvalidConfigurationException
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.datatype.StructType
import org.apache.carbondata.core.metadata.schema.PartitionInfo
import org.apache.carbondata.core.metadata.schema.table.{BucketFields, CarbonTable, DataMapField, DataMapSchema, TableInfo}
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.spark.util.CarbonScalaUtil

case class CarbonCreateTableCommand(
    databaseNameOp: Option[String],
    tableName: String,
    tableProperties: mutable.Map[String, String],
    tableSchema: StructType,
    ifNotExists: Boolean = false,
    bucketFields: Option[BucketFields] = None,
    partitionInfo: Option[PartitionInfo] = None,
    parentTable: Option[CarbonTable] = None,
    dataMapSchemaList: Option[Seq[DataMapSchema]] = None,
    dataMapFields: Option[Map[String, DataMapField]] = None)
  extends RunnableCommand with SchemaProcessCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    processSchema(sparkSession)
  }

  override def processSchema(sparkSession: SparkSession): Seq[Row] = {
    val tableInfo = TableInfo.builder
      .databaseName(CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession))
      .tableName(tableName)
      .tableProperties(tableProperties.asJava)
      .tablePath(CarbonEnv.getTablePath(databaseNameOp, tableName)(sparkSession))
      .metadataFilePath(CarbonEnv.getMetastorePath(databaseNameOp, tableName)(sparkSession))
      .schema(tableSchema)
      .bucketFields(bucketFields.orNull)
      .partitionInfo(partitionInfo.orNull)
      .parentTable(parentTable.orNull)
      .dataMapSchemaList(dataMapSchemaList.getOrElse(Seq.empty).asJava)
      .dataMapFields(dataMapFields.getOrElse(Map.empty).asJava)
      .create()

    CarbonEnv.getInstance(sparkSession).carbonMetastore.checkSchemasModifiedTimeAndReloadTables()
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

    val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    LOGGER.audit(s"Creating Table with Database name [$dbName] and Table name [$tableName]")
    // Add validation for sort scope when create table
    val sortScope = tableInfo.getFactTable.getTableProperties.asScala
      .getOrElse("sort_scope", CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
    if (!CarbonUtil.isValidSortOption(sortScope)) {
      throw new InvalidConfigurationException(
        s"Passing invalid SORT_SCOPE '$sortScope', valid SORT_SCOPE are 'NO_SORT', 'BATCH_SORT'," +
        s" 'LOCAL_SORT' and 'GLOBAL_SORT' ")
    }

    if (tableInfo.getFactTable.getListOfColumns.size <= 0) {
      CarbonException.analysisException("Table should have at least one column.")
    }

    if (sparkSession.sessionState.catalog.listTables(dbName)
      .exists(_.table.equalsIgnoreCase(tableName))) {
      if (!ifNotExists) {
        LOGGER.audit(
          s"Table creation with Database name [$dbName] and Table name [$tableName] failed. " +
          s"Table [$tableName] already exists under database [$dbName]")
        throw new TableAlreadyExistsException(dbName, dbName)
      }
    } else {
      try {
        // TODO: use the original schema ordinal to create table

        val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetastore
        val tablePath = CarbonEnv.getTablePath(databaseNameOp, tableName)(sparkSession)
        val properties =
          if (metaStore.isReadFromHiveMetaStore) {
            CarbonUtil.convertToMultiStringMap(tableInfo)
          } else {
            metaStore.saveToDisk(tableInfo, tablePath)
            new java.util.HashMap[String, String]()
          }

        properties.put("dbName", CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession))
        properties.put("tableName", tableName)
        properties.put("tablePath", tablePath)

        val storage = CatalogStorageFormat.empty.copy(properties = properties.asScala.toMap)
        val schema = CarbonScalaUtil.convertCarbonToSparkDataType(tableSchema)
          .asInstanceOf[SparkStructType]

        val tableDesc = CatalogTable(
          identifier = TableIdentifier(tableName),
          tableType = CatalogTableType.MANAGED,
          storage = storage,
          schema = schema,
          provider = Some("org.apache.spark.sql.CarbonSource"),
          properties = properties.asScala.toMap
        )

        CreateTableCommand(tableDesc, ifNotExists).run(sparkSession)
      } catch {
        case e: AnalysisException => throw e
        case e: Exception =>
          // call the drop table to delete the created table.
          val tableIdentifier =
            AbsoluteTableIdentifier.from(tableInfo.getTablePath, dbName, tableName)
          CarbonEnv.getInstance(sparkSession).carbonMetastore
            .dropTable(tableIdentifier)(sparkSession)
          val msg = s"Create table'$tableName' in database '$dbName' failed."
          LOGGER.audit(msg)
          LOGGER.error(e, msg)
          CarbonException.analysisException(msg)
      }

      LOGGER.audit(s"Table created with Database name [$dbName] and Table name [$tableName]")
    }
    Seq.empty
  }
}
