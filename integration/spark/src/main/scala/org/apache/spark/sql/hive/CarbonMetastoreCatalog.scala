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

package org.apache.spark.sql.hive

import java.io._
import java.util.{GregorianCalendar, UUID}

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.execution.command.{AggregateTableAttributes, CreateHiveTableAsSelectLogicalPlan, Partitioner}
import org.apache.spark.sql.types._

import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.carbon.CarbonTableIdentifier
import org.carbondata.core.carbon.metadata.CarbonMetadata
import org.carbondata.core.carbon.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable
import org.carbondata.core.carbon.path.{CarbonStorePath, CarbonTablePath}
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.datastorage.store.filesystem.CarbonFile
import org.carbondata.core.datastorage.store.impl.FileFactory
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType
import org.carbondata.core.reader.ThriftReader
import org.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.carbondata.core.writer.ThriftWriter
import org.carbondata.format.{SchemaEvolutionEntry, TableInfo}
import org.carbondata.lcm.locks.ZookeeperInit
import org.carbondata.spark.util.CarbonScalaUtil.CarbonSparkUtil
import scala.Array.canBuildFrom
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.parsing.combinator.RegexParsers

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.CreateDataSourceTableUtils._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetOptions}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hive.orc.OrcFileFormat

case class MetaData(var tablesMeta: ArrayBuffer[TableMeta])

case class CarbonMetaData(dims: Seq[String],
  msrs: Seq[String],
  carbonTable: CarbonTable,
  dictionaryMap: DictionaryMap)

case class TableMeta(carbonTableIdentifier: CarbonTableIdentifier, storePath: String,
    var carbonTable: CarbonTable, partitioner: Partitioner)

object CarbonMetastoreCatalog {

  def readSchemaFileToThriftTable(schemaFilePath: String): TableInfo = {
    val createTBase = new ThriftReader.TBaseCreator() {
      override def create(): org.apache.thrift.TBase[TableInfo, TableInfo._Fields] = {
        new TableInfo()
      }
    }
    val thriftReader = new ThriftReader(schemaFilePath, createTBase)
    var tableInfo: TableInfo = null
    try {
      thriftReader.open()
      tableInfo = thriftReader.read().asInstanceOf[TableInfo]
    } finally {
      thriftReader.close()
    }
    tableInfo
  }

  def writeThriftTableToSchemaFile(schemaFilePath: String, tableInfo: TableInfo): Unit = {
    val thriftWriter = new ThriftWriter(schemaFilePath, false)
    try {
      thriftWriter.open()
      thriftWriter.write(tableInfo);
    } finally {
      thriftWriter.close()
    }
  }

}

case class DictionaryMap(dictionaryMap: Map[String, Boolean]) {
  def get(name: String): Option[Boolean] = {
    dictionaryMap.get(name.toLowerCase)
  }
}

class CarbonMetastoreCatalog(sparkSession: SparkSession, val storePath: String)
  extends Logging {

  @transient val LOGGER = LogServiceFactory
    .getLogService("org.apache.spark.sql.CarbonMetastoreCatalog")

  private val sessionState = sparkSession.sessionState.asInstanceOf[CarbonSessionState]
  private val client = sparkSession.sharedState.asInstanceOf[HiveSharedState].metadataHive

  /** A fully qualified identifier for a table (i.e., database.tableName) */
  case class QualifiedTableName(database: String, name: String)

  private def getCurrentDatabase: String = sessionState.catalog.getCurrentDatabase

  def getQualifiedTableName(tableIdent: TableIdentifier): QualifiedTableName = {
    QualifiedTableName(
      tableIdent.database.getOrElse(getCurrentDatabase).toLowerCase,
      tableIdent.table.toLowerCase)
  }

  private def getQualifiedTableName(t: CatalogTable): QualifiedTableName = {
    QualifiedTableName(
      t.identifier.database.getOrElse(getCurrentDatabase).toLowerCase,
      t.identifier.table.toLowerCase)
  }

  /** A cache of Spark SQL data source tables that have been accessed. */
  protected[hive] val cachedDataSourceTables: LoadingCache[QualifiedTableName, LogicalPlan] = {
    val cacheLoader = new CacheLoader[QualifiedTableName, LogicalPlan]() {
      override def load(in: QualifiedTableName): LogicalPlan = {
        logDebug(s"Creating new cached data source for $in")
        val table = client.getTable(in.database, in.name)

        // TODO: the following code is duplicated with FindDataSourceTable.readDataSourceTable

        def schemaStringFromParts: Option[String] = {
          table.properties.get(DATASOURCE_SCHEMA_NUMPARTS).map { numParts =>
            val parts = (0 until numParts.toInt).map { index =>
              val part = table.properties.get(s"$DATASOURCE_SCHEMA_PART_PREFIX$index").orNull
              if (part == null) {
                throw new AnalysisException(
                  "Could not read schema from the metastore because it is corrupted " +
                  s"(missing part $index of the schema, $numParts parts are expected).")
              }

              part
            }
            // Stick all parts back to a single schema string.
            parts.mkString
          }
        }

        def getColumnNames(colType: String): Seq[String] = {
          table.properties.get(s"$DATASOURCE_SCHEMA.num${colType.capitalize}Cols").map {
            numCols => (0 until numCols.toInt).map { index =>
              table.properties.getOrElse(s"$DATASOURCE_SCHEMA_PREFIX${colType}Col.$index",
                throw new AnalysisException(
                  s"Could not read $colType columns from the metastore because it is corrupted " +
                  s"(missing part $index of it, $numCols parts are expected)."))
            }
          }.getOrElse(Nil)
        }

        // Originally, we used spark.sql.sources.schema to store the schema of a data source table.
        // After SPARK-6024, we removed this flag.
        // Although we are not using spark.sql.sources.schema any more, we need to still support.
        val schemaString = table.properties.get(DATASOURCE_SCHEMA).orElse(schemaStringFromParts)

        val userSpecifiedSchema =
          schemaString.map(s => DataType.fromJson(s).asInstanceOf[StructType])

        // We only need names at here since userSpecifiedSchema we loaded from the metastore
        // contains partition columns. We can always get data types of partitioning columns
        // from userSpecifiedSchema.
        val partitionColumns = getColumnNames("part")

        val bucketSpec = table.properties.get(DATASOURCE_SCHEMA_NUMBUCKETS).map { n =>
          BucketSpec(n.toInt, getColumnNames("bucket"), getColumnNames("sort"))
        }

        val options = table.storage.serdeProperties
        val dataSource =
          DataSource(
            sparkSession,
            userSpecifiedSchema = userSpecifiedSchema,
            partitionColumns = partitionColumns,
            bucketSpec = bucketSpec,
            className = table.properties(DATASOURCE_PROVIDER),
            options = options)

        LogicalRelation(
          dataSource.resolveRelation(checkPathExist = true),
          metastoreTableIdentifier = Some(TableIdentifier(in.name, Some(in.database))))
      }
    }

    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  def refreshTable(tableIdent: TableIdentifier): Unit = {
    // refreshTable does not eagerly reload the cache. It just invalidate the cache.
    // Next time when we use the table, it will be populated in the cache.
    // Since we also cache ParquetRelations converted from Hive Parquet tables and
    // adding converted ParquetRelations into the cache is not defined in the load function
    // of the cache (instead, we add the cache entry in convertToParquetRelation),
    // it is better at here to invalidate the cache to avoid confusing waring logs from the
    // cache loader (e.g. cannot find data source provider, which is only defined for
    // data source table.).
    cachedDataSourceTables.invalidate(getQualifiedTableName(tableIdent))
  }

  def hiveDefaultTableFilePath(tableIdent: TableIdentifier): String = {
    // Code based on: hiveWarehouse.getTablePath(currentDatabase, tableName)
    val QualifiedTableName(dbName, tblName) = getQualifiedTableName(tableIdent)
    new Path(new Path(client.getDatabase(dbName).locationUri), tblName).toString
  }

  def lookupRelation(
      tableIdent: TableIdentifier,
      alias: Option[String]): LogicalPlan = {
    val qualifiedTableName = getQualifiedTableName(tableIdent)
    val table = client.getTable(qualifiedTableName.database, qualifiedTableName.name)

    if (table.properties.get(DATASOURCE_PROVIDER).isDefined) {
      val dataSourceTable = cachedDataSourceTables(qualifiedTableName)
      val qualifiedTable = SubqueryAlias(qualifiedTableName.name, dataSourceTable)
      // Then, if alias is specified, wrap the table with a Subquery using the alias.
      // Otherwise, wrap the table with a Subquery using the table name.
      alias.map(a => SubqueryAlias(a, qualifiedTable)).getOrElse(qualifiedTable)
    } else if (table.tableType == CatalogTableType.VIEW) {
      val viewText = table.viewText.getOrElse(sys.error("Invalid view without text."))
      alias match {
        // because hive use things like `_c0` to build the expanded text
        // currently we cannot support view from "create view v1(c1) as ..."
        case None =>
          SubqueryAlias(table.identifier.table,
            sparkSession.sessionState.sqlParser.parsePlan(viewText))
        case Some(aliasText) =>
          SubqueryAlias(aliasText, sessionState.sqlParser.parsePlan(viewText))
      }
    } else {
      MetastoreRelation(
        qualifiedTableName.database, qualifiedTableName.name, alias)(table, client, sparkSession)
    }
  }

  private def getCached(
      tableIdentifier: QualifiedTableName,
      pathsInMetastore: Seq[String],
      metastoreRelation: MetastoreRelation,
      schemaInMetastore: StructType,
      expectedFileFormat: Class[_ <: FileFormat],
      expectedBucketSpec: Option[BucketSpec],
      partitionSpecInMetastore: Option[PartitionSpec]): Option[LogicalRelation] = {

    cachedDataSourceTables.getIfPresent(tableIdentifier) match {
      case null => None // Cache miss
      case logical @ LogicalRelation(relation: HadoopFsRelation, _, _) =>
        val cachedRelationFileFormatClass = relation.fileFormat.getClass

        expectedFileFormat match {
          case `cachedRelationFileFormatClass` =>
            // If we have the same paths, same schema, and same partition spec,
            // we will use the cached relation.
            val useCached =
              relation.location.paths.map(_.toString).toSet == pathsInMetastore.toSet &&
              logical.schema.sameType(schemaInMetastore) &&
              relation.bucketSpec == expectedBucketSpec &&
              relation.partitionSpec == partitionSpecInMetastore.getOrElse {
                PartitionSpec(StructType(Nil), Array.empty[PartitionDirectory])
              }

            if (useCached) {
              Some(logical)
            } else {
              // If the cached relation is not updated, we invalidate it right away.
              cachedDataSourceTables.invalidate(tableIdentifier)
              None
            }
          case _ =>
            logWarning(
              s"${metastoreRelation.databaseName}.${metastoreRelation.tableName} " +
              s"should be stored as $expectedFileFormat. However, we are getting " +
              s"a ${relation.fileFormat} from the metastore cache. This cached " +
              s"entry will be invalidated.")
            cachedDataSourceTables.invalidate(tableIdentifier)
            None
        }
      case other =>
        logWarning(
          s"${metastoreRelation.databaseName}.${metastoreRelation.tableName} should be stored " +
          s"as $expectedFileFormat. However, we are getting a $other from the metastore cache. " +
          s"This cached entry will be invalidated.")
        cachedDataSourceTables.invalidate(tableIdentifier)
        None
    }
  }

  private def convertToLogicalRelation(
      metastoreRelation: MetastoreRelation,
      options: Map[String, String],
      defaultSource: FileFormat,
      fileFormatClass: Class[_ <: FileFormat],
      fileType: String): LogicalRelation = {
    val metastoreSchema = StructType.fromAttributes(metastoreRelation.output)
    val tableIdentifier =
      QualifiedTableName(metastoreRelation.databaseName, metastoreRelation.tableName)
    val bucketSpec = None  // We don't support hive bucketed tables, only ones we write out.

    val result = if (metastoreRelation.hiveQlTable.isPartitioned) {
      val partitionSchema = StructType.fromAttributes(metastoreRelation.partitionKeys)
      val partitionColumnDataTypes = partitionSchema.map(_.dataType)
      // We're converting the entire table into HadoopFsRelation, so predicates to Hive metastore
      // are empty.
      val partitions = metastoreRelation.getHiveQlPartitions().map { p =>
        val location = p.getLocation
        val values = InternalRow.fromSeq(p.getValues.asScala.zip(partitionColumnDataTypes).map {
          case (rawValue, dataType) => Cast(Literal(rawValue), dataType).eval(null)
        })
        PartitionDirectory(values, location)
      }
      val partitionSpec = PartitionSpec(partitionSchema, partitions)
      val partitionPaths = partitions.map(_.path.toString)

      // By convention (for example, see MetaStorePartitionedTableFileCatalog), the definition of a
      // partitioned table's paths depends on whether that table has any actual partitions.
      // Partitioned tables without partitions use the location of the table's base path.
      // Partitioned tables with partitions use the locations of those partitions' data locations,
      // _omitting_ the table's base path.
      val paths = if (partitionPaths.isEmpty) {
        Seq(metastoreRelation.hiveQlTable.getDataLocation.toString)
      } else {
        partitionPaths
      }

      val cached = getCached(
        tableIdentifier,
        paths,
        metastoreRelation,
        metastoreSchema,
        fileFormatClass,
        bucketSpec,
        Some(partitionSpec))

      val hadoopFsRelation = cached.getOrElse {
        val fileCatalog = new MetaStorePartitionedTableFileCatalog(
          sparkSession,
          new Path(metastoreRelation.catalogTable.storage.locationUri.get),
          partitionSpec)

        val inferredSchema = if (fileType.equals("parquet")) {
          val inferredSchema =
            defaultSource.inferSchema(sparkSession, options, fileCatalog.allFiles())
          inferredSchema.map { inferred =>
            ParquetFileFormat.mergeMetastoreParquetSchema(metastoreSchema, inferred)
          }.getOrElse(metastoreSchema)
        } else {
          defaultSource.inferSchema(sparkSession, options, fileCatalog.allFiles()).get
        }

        val relation = HadoopFsRelation(
          sparkSession = sparkSession,
          location = fileCatalog,
          partitionSchema = partitionSchema,
          dataSchema = inferredSchema,
          bucketSpec = bucketSpec,
          fileFormat = defaultSource,
          options = options)

        val created = LogicalRelation(
          relation,
          metastoreTableIdentifier =
            Some(TableIdentifier(tableIdentifier.name, Some(tableIdentifier.database))))
        cachedDataSourceTables.put(tableIdentifier, created)
        created
      }

      hadoopFsRelation
    } else {
      val paths = Seq(metastoreRelation.hiveQlTable.getDataLocation.toString)

      val cached = getCached(tableIdentifier,
        paths,
        metastoreRelation,
        metastoreSchema,
        fileFormatClass,
        bucketSpec,
        None)
      val logicalRelation = cached.getOrElse {
        val created =
          LogicalRelation(
            DataSource(
              sparkSession = sparkSession,
              paths = paths,
              userSpecifiedSchema = Some(metastoreRelation.schema),
              bucketSpec = bucketSpec,
              options = options,
              className = fileType).resolveRelation(),
            metastoreTableIdentifier =
              Some(TableIdentifier(tableIdentifier.name, Some(tableIdentifier.database))))


        cachedDataSourceTables.put(tableIdentifier, created)
        created
      }

      logicalRelation
    }
    result.copy(expectedOutputAttributes = Some(metastoreRelation.output))
  }

  /**
   * When scanning or writing to non-partitioned Metastore Parquet tables, convert them to Parquet
   * data source relations for better performance.
   */
  object ParquetConversions extends Rule[LogicalPlan] {
    private def shouldConvertMetastoreParquet(relation: MetastoreRelation): Boolean = {
      relation.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") &&
      sessionState.convertMetastoreParquet
    }

    private def convertToParquetRelation(relation: MetastoreRelation): LogicalRelation = {
      val defaultSource = new ParquetFileFormat()
      val fileFormatClass = classOf[ParquetFileFormat]

      val mergeSchema = sessionState.convertMetastoreParquetWithSchemaMerging
      val options = Map(ParquetOptions.MERGE_SCHEMA -> mergeSchema.toString)

      convertToLogicalRelation(relation, options, defaultSource, fileFormatClass, "parquet")
    }

    override def apply(plan: LogicalPlan): LogicalPlan = {
      if (!plan.resolved || plan.analyzed) {
        return plan
      }

      plan transformUp {
        // Write path
        case InsertIntoTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
          // Inserting into partitioned table is not supported in Parquet data source (yet).
          if !r.hiveQlTable.isPartitioned && shouldConvertMetastoreParquet(r) =>
          InsertIntoTable(convertToParquetRelation(r), partition, child, overwrite, ifNotExists)

        // Write path
        case InsertIntoHiveTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
          // Inserting into partitioned table is not supported in Parquet data source (yet).
          if !r.hiveQlTable.isPartitioned && shouldConvertMetastoreParquet(r) =>
          InsertIntoTable(convertToParquetRelation(r), partition, child, overwrite, ifNotExists)

        // Read path
        case relation: MetastoreRelation if shouldConvertMetastoreParquet(relation) =>
          val parquetRelation = convertToParquetRelation(relation)
          SubqueryAlias(relation.alias.getOrElse(relation.tableName), parquetRelation)
      }
    }
  }

  /**
   * When scanning Metastore ORC tables, convert them to ORC data source relations
   * for better performance.
   */
  object OrcConversions extends Rule[LogicalPlan] {
    private def shouldConvertMetastoreOrc(relation: MetastoreRelation): Boolean = {
      relation.tableDesc.getSerdeClassName.toLowerCase.contains("orc") &&
      sessionState.convertMetastoreOrc
    }

    private def convertToOrcRelation(relation: MetastoreRelation): LogicalRelation = {
      val defaultSource = new OrcFileFormat()
      val fileFormatClass = classOf[OrcFileFormat]
      val options = Map[String, String]()

      convertToLogicalRelation(relation, options, defaultSource, fileFormatClass, "orc")
    }

    override def apply(plan: LogicalPlan): LogicalPlan = {
      if (!plan.resolved || plan.analyzed) {
        return plan
      }

      plan transformUp {
        // Write path
        case InsertIntoTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
          // Inserting into partitioned table is not supported in Orc data source (yet).
          if !r.hiveQlTable.isPartitioned && shouldConvertMetastoreOrc(r) =>
          InsertIntoTable(convertToOrcRelation(r), partition, child, overwrite, ifNotExists)

        // Write path
        case InsertIntoHiveTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
          // Inserting into partitioned table is not supported in Orc data source (yet).
          if !r.hiveQlTable.isPartitioned && shouldConvertMetastoreOrc(r) =>
          InsertIntoTable(convertToOrcRelation(r), partition, child, overwrite, ifNotExists)

        // Read path
        case relation: MetastoreRelation if shouldConvertMetastoreOrc(relation) =>
          val orcRelation = convertToOrcRelation(relation)
          SubqueryAlias(relation.alias.getOrElse(relation.tableName), orcRelation)
      }
    }
  }

  /**
   * Creates any tables required for query execution.
   * For example, because of a CREATE TABLE X AS statement.
   */
  object CreateTables extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p
      case p: LogicalPlan if p.resolved => p

      case p @ CreateHiveTableAsSelectLogicalPlan(table, child, allowExisting) =>
        val desc = if (table.storage.serde.isEmpty) {
          // add default serde
          table.withNewStorage(
            serde = Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
        } else {
          table
        }

        val QualifiedTableName(dbName, tblName) = getQualifiedTableName(table)

        execution.CreateHiveTableAsSelectCommand(
          desc.copy(identifier = TableIdentifier(tblName, Some(dbName))),
          child,
          allowExisting)
    }
  }


  val tableModifiedTimeStore = new java.util.HashMap[String, Long]()
  tableModifiedTimeStore.put("default", System.currentTimeMillis())

  val metadata = loadMetadata(storePath)

  def getTableCreationTime(databaseName: String, tableName: String): Long = {
    val tableMeta = metadata.tablesMeta.filter(
      c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(databaseName) &&
           c.carbonTableIdentifier.getTableName.equalsIgnoreCase(tableName))
    val tableCreationTime = tableMeta.head.carbonTable.getTableLastUpdatedTime
    tableCreationTime
  }


  def lookupRelation1(dbName: Option[String],
      tableName: String)(sqlContext: SparkSession): CarbonDatasourceRelation = {
    lookupRelation1(TableIdentifier(tableName, dbName))(sqlContext)
  }

  def lookupRelation1(tableIdentifier: TableIdentifier,
      alias: Option[String] = None)(sqlContext: SparkSession): CarbonDatasourceRelation = {
    checkSchemasModifiedTimeAndReloadTables()
    val database = tableIdentifier.database.getOrElse(getDB.getDatabaseName(None, sparkSession))
    val tables = metadata.tablesMeta.filter(
      c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(database) &&
           c.carbonTableIdentifier.getTableName.equalsIgnoreCase(tableIdentifier.table))
    if (tables.nonEmpty) {
      CarbonDatasourceRelation(tableIdentifier, alias)(sqlContext)
    } else {
      LOGGER.audit(s"Table Not Found: ${tableIdentifier.table}")
      throw new NoSuchTableException(database, tableIdentifier.table)
    }
  }

  def getMetaData(identifier: TableIdentifier): CarbonMetaData = {
    checkSchemasModifiedTimeAndReloadTables()
    val database = identifier.database.getOrElse(getDB.getDatabaseName(None, sparkSession))
    val tables = metadata.tablesMeta.filter(
      c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(database) &&
          c.carbonTableIdentifier.getTableName.equalsIgnoreCase(identifier.table))
    if (tables.nonEmpty) {
      CarbonSparkUtil.createSparkMeta(tables.head.carbonTable)
    } else {
      LOGGER.error(s"Table Not Found: ${identifier.table}")
      throw new NoSuchTableException(database, identifier.table)
    }
  }

  def getTableMeta(identifier: TableIdentifier): TableMeta = {
    checkSchemasModifiedTimeAndReloadTables()
    val database = identifier.database.getOrElse(getDB.getDatabaseName(None, sparkSession))
    val tables = metadata.tablesMeta.filter(
      c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(database) &&
          c.carbonTableIdentifier.getTableName.equalsIgnoreCase(identifier.table))
    if (tables.nonEmpty) {
      tables.head
    } else {
      LOGGER.error(s"Table Not Found: ${identifier.table}")
      throw new NoSuchTableException(database, identifier.table)
    }
  }

  def tableExists(tableIdentifier: TableIdentifier): Boolean = {
    checkSchemasModifiedTimeAndReloadTables()
    val database = tableIdentifier.database.getOrElse(getDB.getDatabaseName(None, sparkSession))
    val tables = metadata.tablesMeta.filter(
      c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(database) &&
           c.carbonTableIdentifier.getTableName.equalsIgnoreCase(tableIdentifier.table))
    tables.nonEmpty
  }

  def loadMetadata(metadataPath: String): MetaData = {

    // creating zookeeper instance once.
    // if zookeeper is configured as carbon lock type.
    if (CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.LOCK_TYPE, CarbonCommonConstants.LOCK_TYPE_DEFAULT)
      .equalsIgnoreCase(CarbonCommonConstants.CARBON_LOCK_TYPE_ZOOKEEPER)) {
      val zookeeperUrl = sparkSession.sqlContext
          .getConf("spark.deploy.zookeeper.url", "127.0.0.1:2181")
      ZookeeperInit.getInstance(zookeeperUrl)
    }

    if (metadataPath == null) {
      return null
    }
    val fileType = FileFactory.getFileType(metadataPath)
    val metaDataBuffer = new ArrayBuffer[TableMeta]
    fillMetaData(metadataPath, fileType, metaDataBuffer)
    updateSchemasUpdatedTime("", "")
    MetaData(metaDataBuffer)

  }

  private def fillMetaData(basePath: String, fileType: FileType,
      metaDataBuffer: ArrayBuffer[TableMeta]): Unit = {
    val databasePath = basePath // + "/schemas"
    try {
      if (FileFactory.isFileExist(databasePath, fileType)) {
        val file = FileFactory.getCarbonFile(databasePath, fileType)
        val databaseFolders = file.listFiles()

        databaseFolders.foreach(databaseFolder => {
          if (databaseFolder.isDirectory) {
            val dbName = databaseFolder.getName
            val tableFolders = databaseFolder.listFiles()

            tableFolders.foreach(tableFolder => {
              if (tableFolder.isDirectory) {
                val carbonTableIdentifier = new CarbonTableIdentifier(databaseFolder.getName,
                    tableFolder.getName, UUID.randomUUID().toString)
                val carbonTablePath = CarbonStorePath.getCarbonTablePath(basePath,
                  carbonTableIdentifier)
                val tableMetadataFile = carbonTablePath.getSchemaFilePath

                if (FileFactory.isFileExist(tableMetadataFile, fileType)) {
                  val tableName = tableFolder.getName
                  val tableUniqueName = databaseFolder.getName + "_" + tableFolder.getName


                  val createTBase = new ThriftReader.TBaseCreator() {
                    override def create(): org.apache.thrift.TBase[TableInfo, TableInfo._Fields] = {
                      new TableInfo()
                    }
                  }
                  val thriftReader = new ThriftReader(tableMetadataFile, createTBase)
                  thriftReader.open()
                  val tableInfo: TableInfo = thriftReader.read().asInstanceOf[TableInfo]
                  thriftReader.close()

                  val schemaConverter = new ThriftWrapperSchemaConverterImpl
                  val wrapperTableInfo = schemaConverter
                    .fromExternalToWrapperTableInfo(tableInfo, dbName, tableName, basePath)
                  val schemaFilePath = CarbonStorePath
                    .getCarbonTablePath(storePath, carbonTableIdentifier).getSchemaFilePath
                  wrapperTableInfo.setStorePath(storePath)
                  wrapperTableInfo
                    .setMetaDataFilepath(CarbonTablePath.getFolderContainingFile(schemaFilePath))
                  CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo)
                  val carbonTable = org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
                      .getCarbonTable(tableUniqueName)
                  metaDataBuffer += TableMeta(
                    carbonTable.getCarbonTableIdentifier,
                    storePath,
                    carbonTable,
                    // TODO: Need to update Database thirft to hold partitioner
                    // information and reload when required.
                    Partitioner("org.carbondata.spark.partition.api.impl." +
                                "SampleDataPartitionerImpl",
                      Array(""), 1, Array("")))
                }
              }
            })
          }
        })
      }
      else {
        // Create folders and files.
        FileFactory.mkdirs(databasePath, fileType)

      }
    }
    catch {
      case s: java.io.FileNotFoundException =>
        // Create folders and files.
        FileFactory.mkdirs(databasePath, fileType)

    }
  }


  /**
   *
   * Prepare Thrift Schema from wrapper TableInfo and write to Schema file.
   * Load CarbonTable from wrapper tableinfo
   *
   */
  def createTableFromThrift(
      tableInfo: org.carbondata.core.carbon.metadata.schema.table.TableInfo,
      dbName: String,
      tableName: String,
      partitioner: Partitioner)
    (sqlContext: SparkSession): String = {

    if (tableExists(TableIdentifier(tableName, Some(dbName)))) {
      sys.error(s"Table [$tableName] already exists under Database [$dbName]")
    }

    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val thriftTableInfo =
      schemaConverter.fromWrapperToExternalTableInfo(tableInfo, dbName, tableName)
    val schemaEvolutionEntry = new SchemaEvolutionEntry(tableInfo.getLastUpdatedTime)
    thriftTableInfo.getFact_table.getSchema_evolution.getSchema_evolution_history
      .add(schemaEvolutionEntry)

    val carbonTableIdentifier = new CarbonTableIdentifier(dbName, tableName,
        tableInfo.getFactTable.getTableId)
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(storePath, carbonTableIdentifier)
    val schemaFilePath = carbonTablePath.getSchemaFilePath
    val schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath)
    tableInfo.setMetaDataFilepath(schemaMetadataPath)
    tableInfo.setStorePath(storePath)
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo)

    val tableMeta = TableMeta(
      carbonTableIdentifier,
      storePath,
      CarbonMetadata.getInstance().getCarbonTable(dbName + "_" + tableName),
      Partitioner("org.carbondata.spark.partition.api.impl.SampleDataPartitionerImpl",
        Array(""), 1, DistributionUtil.getNodeList(sparkSession.sparkContext)))

    val fileType = FileFactory.getFileType(schemaMetadataPath)
    if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
      FileFactory.mkdirs(schemaMetadataPath, fileType)
    }

    val thriftWriter = new ThriftWriter(schemaFilePath, false)
    thriftWriter.open()
    thriftWriter.write(thriftTableInfo)
    thriftWriter.close()

    metadata.tablesMeta += tableMeta
    logInfo(s"Table $tableName for Database $dbName created successfully.")
    LOGGER.info("Table " + tableName + " for Database " + dbName + " created successfully.")
    updateSchemasUpdatedTime(dbName, tableName)
    carbonTablePath.getPath
  }

  private def updateMetadataByWrapperTable(
      wrapperTableInfo: org.carbondata.core.carbon.metadata.schema.table.TableInfo): Unit = {

    CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo)
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(
      wrapperTableInfo.getTableUniqueName)
    for (i <- metadata.tablesMeta.indices) {
      if (wrapperTableInfo.getTableUniqueName.equals(
        metadata.tablesMeta(i).carbonTableIdentifier.getTableUniqueName)) {
        metadata.tablesMeta(i).carbonTable = carbonTable
      }
    }
  }

  def updateMetadataByThriftTable(schemaFilePath: String,
      tableInfo: TableInfo, dbName: String, tableName: String, storePath: String): Unit = {

    tableInfo.getFact_table.getSchema_evolution.getSchema_evolution_history.get(0)
      .setTime_stamp(System.currentTimeMillis())
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val wrapperTableInfo = schemaConverter
      .fromExternalToWrapperTableInfo(tableInfo, dbName, tableName, storePath)
    wrapperTableInfo
      .setMetaDataFilepath(CarbonTablePath.getFolderContainingFile(schemaFilePath))
    wrapperTableInfo.setStorePath(storePath)
    updateMetadataByWrapperTable(wrapperTableInfo)
  }


  def getDimensions(carbonTable: CarbonTable,
      aggregateAttributes: List[AggregateTableAttributes]): Array[String] = {
    var dimArray = Array[String]()
    aggregateAttributes.filter { agg => null == agg.aggType }.foreach { agg =>
      val colName = agg.colName
      if (null != carbonTable.getMeasureByName(carbonTable.getFactTableName, colName)) {
        sys
          .error(s"Measure must be provided along with aggregate function :: $colName")
      }
      if (null == carbonTable.getDimensionByName(carbonTable.getFactTableName, colName)) {
        sys
          .error(s"Invalid column name. Cannot create an aggregate table :: $colName")
      }
      if (dimArray.contains(colName)) {
        sys.error(s"Duplicate column name. Cannot create an aggregate table :: $colName")
      }
      dimArray :+= colName
    }
    dimArray
  }

  /**
   * Shows all schemas which has Database name like
   */
  def showDatabases(schemaLike: Option[String]): Seq[String] = {
    checkSchemasModifiedTimeAndReloadTables()
    metadata.tablesMeta.map { c =>
      schemaLike match {
        case Some(name) =>
          if (c.carbonTableIdentifier.getDatabaseName.contains(name)) {
            c.carbonTableIdentifier
              .getDatabaseName
          }
          else {
            null
          }
        case _ => c.carbonTableIdentifier.getDatabaseName
      }
    }.filter(f => f != null)
  }

  /**
   * Shows all tables for given schema.
   */
  def getTables(databaseName: Option[String])(sqlContext: SQLContext): Seq[(String, Boolean)] = {

    val dbName = databaseName.getOrElse(sqlContext.sparkSession.catalog.currentDatabase)
    checkSchemasModifiedTimeAndReloadTables()
    metadata.tablesMeta.filter { c =>
      c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(dbName)
    }.map { c => (c.carbonTableIdentifier.getTableName, false) }
  }

  /**
   * Shows all tables in all schemas.
   */
  def getAllTables()(sqlContext: SQLContext): Seq[TableIdentifier] = {
    checkSchemasModifiedTimeAndReloadTables()
    metadata.tablesMeta.map { c =>
        TableIdentifier(c.carbonTableIdentifier.getTableName,
          Some(c.carbonTableIdentifier.getDatabaseName))
    }
  }

  def dropTable(partitionCount: Int, tableStorePath: String, tableIdentifier: TableIdentifier)
    (sqlContext: SparkSession) {
    val dbName = tableIdentifier.database.get
    val tableName = tableIdentifier.table
    if (!tableExists(tableIdentifier)) {
      LOGGER.audit(s"Drop Table failed. Table with $dbName.$tableName does not exist")
      sys.error(s"Table with $dbName.$tableName does not exist")
    }

    val carbonTable = org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
      .getCarbonTable(dbName + "_" + tableName)

    if (null != carbonTable) {
      val metadatFilePath = carbonTable.getMetaDataFilepath
      val fileType = FileFactory.getFileType(metadatFilePath)

      if (FileFactory.isFileExist(metadatFilePath, fileType)) {
        val file = FileFactory.getCarbonFile(metadatFilePath, fileType)
        CarbonUtil.renameTableForDeletion(partitionCount, tableStorePath, dbName, tableName)
        CarbonUtil.deleteFoldersAndFilesSilent(file.getParentFile)
      }

      val partitionLocation = tableStorePath + File.separator + "partition" + File.separator +
                              dbName + File.separator + tableName
      val partitionFileType = FileFactory.getFileType(partitionLocation)
      if (FileFactory.isFileExist(partitionLocation, partitionFileType)) {
        CarbonUtil
          .deleteFoldersAndFiles(FileFactory.getCarbonFile(partitionLocation, partitionFileType))
      }
    }

    metadata.tablesMeta -= metadata.tablesMeta.filter(
      c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(dbName) &&
           c.carbonTableIdentifier.getTableName.equalsIgnoreCase(tableName))(0)
    org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
      .removeTable(dbName + "_" + tableName)

    try {
      sqlContext.sql(s"DROP TABLE $dbName.$tableName").collect()
    } catch {
      case e: Exception =>
        LOGGER.audit(
          s"Error While deleting the table $dbName.$tableName during drop Table" + e.getMessage)
    }
    logInfo(s"Table $tableName of $dbName Database dropped syccessfully.")
    LOGGER.info("Table " + tableName + " of " + dbName + " Database dropped syccessfully.")

  }

  private def getTimestampFileAndType(databaseName: String, tableName: String) = {

    val timestampFile = storePath + "/" + CarbonCommonConstants.SCHEMAS_MODIFIED_TIME_FILE

    val timestampFileType = FileFactory.getFileType(timestampFile)
    (timestampFile, timestampFileType)
  }

  def updateSchemasUpdatedTime(databaseName: String, tableName: String) {
    val (timestampFile, timestampFileType) = getTimestampFileAndType(databaseName, tableName)

    if (!FileFactory.isFileExist(timestampFile, timestampFileType)) {
      LOGGER.audit(s"Creating timestamp file for $databaseName.$tableName")
      FileFactory.createNewFile(timestampFile, timestampFileType)
    }

    touchSchemasTimestampFile(databaseName, tableName)

    tableModifiedTimeStore.put("default",
      FileFactory.getCarbonFile(timestampFile, timestampFileType).getLastModifiedTime)

  }

  def touchSchemasTimestampFile(databaseName: String, tableName: String) {
    val (timestampFile, timestampFileType) = getTimestampFileAndType(databaseName, tableName)
    FileFactory.getCarbonFile(timestampFile, timestampFileType)
      .setLastModifiedTime(System.currentTimeMillis())
  }

  def checkSchemasModifiedTimeAndReloadTables() {
    val (timestampFile, timestampFileType) = getTimestampFileAndType("", "")
    if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
      if (!(FileFactory.getCarbonFile(timestampFile, timestampFileType).
        getLastModifiedTime == tableModifiedTimeStore.get("default"))) {
        refreshCache()
      }
    }
  }

  def refreshCache() {
    metadata.tablesMeta = loadMetadata(storePath).tablesMeta
  }

  def getSchemaLastUpdatedTime(databaseName: String, tableName: String): Long = {
    var schemaLastUpdatedTime = System.currentTimeMillis
    val (timestampFile, timestampFileType) = getTimestampFileAndType(databaseName, tableName)
    if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
      schemaLastUpdatedTime = FileFactory.getCarbonFile(timestampFile, timestampFileType)
        .getLastModifiedTime
    }
    schemaLastUpdatedTime
  }

  def readTableMetaDataFile(tableFolder: CarbonFile,
      fileType: FileFactory.FileType):
  (String, String, String, String, Partitioner, Long) = {
    val tableMetadataFile = tableFolder.getAbsolutePath + "/metadata"

    var schema: String = ""
    var databaseName: String = ""
    var tableName: String = ""
    var dataPath: String = ""
    var partitioner: Partitioner = null
    val cal = new GregorianCalendar(2011, 1, 1)
    var tableCreationTime = cal.getTime.getTime

    if (FileFactory.isFileExist(tableMetadataFile, fileType)) {
      // load metadata
      val in = FileFactory.getDataInputStream(tableMetadataFile, fileType)
      var len = 0
      try {
        len = in.readInt()
      } catch {
        case others: EOFException => len = 0
      }

      while (len > 0) {
        val databaseNameBytes = new Array[Byte](len)
        in.readFully(databaseNameBytes)

        databaseName = new String(databaseNameBytes, "UTF8")
        val tableNameLen = in.readInt()
        val tableNameBytes = new Array[Byte](tableNameLen)
        in.readFully(tableNameBytes)
        tableName = new String(tableNameBytes, "UTF8")

        val dataPathLen = in.readInt()
        val dataPathBytes = new Array[Byte](dataPathLen)
        in.readFully(dataPathBytes)
        dataPath = new String(dataPathBytes, "UTF8")

        val versionLength = in.readInt()
        val versionBytes = new Array[Byte](versionLength)
        in.readFully(versionBytes)

        val schemaLen = in.readInt()
        val schemaBytes = new Array[Byte](schemaLen)
        in.readFully(schemaBytes)
        schema = new String(schemaBytes, "UTF8")

        val partitionLength = in.readInt()
        val partitionBytes = new Array[Byte](partitionLength)
        in.readFully(partitionBytes)
        val inStream = new ByteArrayInputStream(partitionBytes)
        val objStream = new ObjectInputStream(inStream)
        partitioner = objStream.readObject().asInstanceOf[Partitioner]
        objStream.close()

        try {
          tableCreationTime = in.readLong()
          len = in.readInt()
        } catch {
          case others: EOFException => len = 0
        }

      }
      in.close()
    }

    (databaseName, tableName, dataPath, schema, partitioner, tableCreationTime)
  }

}


object CarbonMetastoreTypes extends RegexParsers {
  protected lazy val primitiveType: Parser[DataType] =
    "string" ^^^ StringType |
      "float" ^^^ FloatType |
      "int" ^^^ IntegerType |
      "tinyint" ^^^ ShortType |
      "short" ^^^ ShortType |
      "double" ^^^ DoubleType |
      "long" ^^^ LongType |
      "binary" ^^^ BinaryType |
      "boolean" ^^^ BooleanType |
      fixedDecimalType |
      "decimal" ^^^ "decimal" ^^^ DecimalType(18, 2) |
      "varchar\\((\\d+)\\)".r ^^^ StringType |
      "timestamp" ^^^ TimestampType

  protected lazy val fixedDecimalType: Parser[DataType] =
    "decimal" ~> "(" ~> "^[1-9]\\d*".r ~ ("," ~> "^[0-9]\\d*".r <~ ")") ^^ {
      case precision ~ scale =>
        DecimalType(precision.toInt, scale.toInt)
    }

  protected lazy val arrayType: Parser[DataType] =
    "array" ~> "<" ~> dataType <~ ">" ^^ {
      case tpe => ArrayType(tpe)
    }

  protected lazy val mapType: Parser[DataType] =
    "map" ~> "<" ~> dataType ~ "," ~ dataType <~ ">" ^^ {
      case t1 ~ _ ~ t2 => MapType(t1, t2)
    }

  protected lazy val structField: Parser[StructField] =
    "[a-zA-Z0-9_]*".r ~ ":" ~ dataType ^^ {
      case name ~ _ ~ tpe => StructField(name, tpe, nullable = true)
    }

  protected lazy val structType: Parser[DataType] =
    "struct" ~> "<" ~> repsep(structField, ",") <~ ">" ^^ {
      case fields => StructType(fields)
    }

  protected lazy val dataType: Parser[DataType] =
    arrayType |
    mapType |
    structType |
    primitiveType

  def toDataType(metastoreType: String): DataType = {
    parseAll(dataType, metastoreType) match {
      case Success(result, _) => result
      case failure: NoSuccess => sys.error(s"Unsupported dataType: $metastoreType")
    }
  }

  def toMetastoreType(dt: DataType): String = {
    dt match {
      case ArrayType(elementType, _) => s"array<${ toMetastoreType(elementType) }>"
      case StructType(fields) =>
        s"struct<${
          fields.map(f => s"${ f.name }:${ toMetastoreType(f.dataType) }")
            .mkString(",")
        }>"
      case StringType => "string"
      case FloatType => "float"
      case IntegerType => "int"
      case ShortType => "tinyint"
      case DoubleType => "double"
      case LongType => "bigint"
      case BinaryType => "binary"
      case BooleanType => "boolean"
      case DecimalType() => "decimal"
      case TimestampType => "timestamp"
    }
  }

}
