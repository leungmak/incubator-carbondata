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

package org.apache.spark.sql.execution.command.schema

import scala.collection.JavaConverters._
import scala.collection.mutable.Map

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.{AlterTableColumnSchemaGenerator, RunnableCommand}
import org.apache.spark.sql.hive.CarbonSessionState
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.locks.{ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.datatype.StructField
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.events.{AlterTableAddColumnPreEvent, OperationListenerBus}
import org.apache.carbondata.format
import org.apache.carbondata.spark.rdd.{AlterTableAddColumnRDD, AlterTableDropColumnRDD}

private[sql] case class CarbonAlterTableAddColumnCommand(
    databaseNameOp: Option[String],
    tableName: String,
    newFields: Seq[StructField],
    tableProperties: Map[String, String])
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    LOGGER.audit(s"Alter table add columns request has been received for $dbName.$tableName")
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK, LockUsage.COMPACTION_LOCK)
    var locks = List.empty[ICarbonLock]
    var timeStamp = 0L
    var newCols = Seq[org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema]()
    var carbonTable: CarbonTable = null
    try {
      locks = AlterTableUtil
        .validateTableAndAcquireLock(dbName, tableName, locksToBeAcquired)(sparkSession)
      // Consider a concurrent scenario where 2 alter operations are executed in parallel. 1st
      // operation is success and updates the schema file. 2nd operation will get the lock after
      // completion of 1st operation but as look up relation is called before it will have the
      // older carbon table and this can lead to inconsistent state in the system. Therefor look
      // up relation should be called after acquiring the lock
      val metastore = CarbonEnv.getInstance(sparkSession).carbonMetastore
      carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
      val alterTableAddColumnListener = AlterTableAddColumnPreEvent(carbonTable)
      OperationListenerBus.getInstance().fireEvent(alterTableAddColumnListener)
      // get the latest carbon table and check for column existence
      // read the latest schema file
      val carbonTablePath =
        CarbonStorePath.getCarbonTablePath(carbonTable.getAbsoluteTableIdentifier)
      val thriftTableInfo: format.TableInfo =
        metastore.getThriftTableInfo(carbonTablePath)(sparkSession)
      val schemaConverter = new ThriftWrapperSchemaConverterImpl()
      val wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(
          thriftTableInfo,
          dbName,
          tableName,
          carbonTable.getTablePath)

      newCols = new AlterTableColumnSchemaGenerator(
          dbName,
          tableName,
          newFields,
          tableProperties,
          carbonTable,
          carbonTablePath,
          carbonTable.getCarbonTableIdentifier,
          sparkSession.sparkContext).process
      // generate dictionary files for the newly added columns
      new AlterTableAddColumnRDD(sparkSession.sparkContext,
        newCols,
        carbonTable.getAbsoluteTableIdentifier).collect()
      timeStamp = System.currentTimeMillis
      val schemaEvolutionEntry = new org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry
      schemaEvolutionEntry.setTimeStamp(timeStamp)
      schemaEvolutionEntry.setAdded(newCols.toList.asJava)
      val thriftTable = schemaConverter
        .fromWrapperToExternalTableInfo(wrapperTableInfo, dbName, tableName)
      AlterTableUtil.updateSchemaInfo(
        carbonTable,
        schemaConverter.fromWrapperToExternalSchemaEvolutionEntry(schemaEvolutionEntry),
        thriftTable)(sparkSession,
        sparkSession.sessionState.asInstanceOf[CarbonSessionState])
      LOGGER.info(s"Alter table for add columns is successful for table $dbName.$tableName")
      LOGGER.audit(s"Alter table for add columns is successful for table $dbName.$tableName")
    } catch {
      case e: Exception =>
        LOGGER.error(e, "Alter table add columns failed")
        if (newCols.nonEmpty) {
          LOGGER.info("Cleaning up the dictionary files as alter table add operation failed")
          new AlterTableDropColumnRDD(sparkSession.sparkContext,
            newCols,
            carbonTable.getAbsoluteTableIdentifier).collect()
          AlterTableUtil.revertAddColumnChanges(dbName, tableName, timeStamp)(sparkSession)
        }
        sys.error(s"Alter table add operation failed: ${e.getMessage}")
    } finally {
      // release lock after command execution completion
      AlterTableUtil.releaseLocks(locks)
    }
    Seq.empty
  }
}
