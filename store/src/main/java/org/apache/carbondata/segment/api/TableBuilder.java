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

package org.apache.carbondata.segment.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.datatype.StructType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.ThriftWriter;
import org.apache.carbondata.format.SchemaEvolutionEntry;
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException;

public class TableBuilder {

  LogService LOGGER = LogServiceFactory.getLogService(this.getClass().getCanonicalName());

  // following members are required to build TableInfo

  private String databaseName;
  private String tableName;
  private String tablePath;
  private TableSchema tableSchema;

  // parent table object, which can be used during dropping of pre-aggreate table as
  // parent table will also get updated
  private CarbonTable parentTable;

  private TableBuilder() { }

  public static TableBuilder newInstance() {
    return new TableBuilder();
  }

  public Table create() throws IOException {
    if (tableName == null || tablePath == null || tableSchema == null) {
      throw new IllegalArgumentException("must provide table name and table path");
    }

    if (databaseName == null) {
      databaseName = "default";
    }

    TableInfo tableInfo = new TableInfo();
    tableInfo.setDatabaseName(databaseName);
    tableInfo.setTableUniqueName(databaseName + "_" + tableName);
    tableInfo.setFactTable(tableSchema);
    tableInfo.setTablePath(tablePath);
    tableInfo.setLastUpdatedTime(System.currentTimeMillis());
    tableInfo.setDataMapSchemaList(new ArrayList<DataMapSchema>(0));
    AbsoluteTableIdentifier identifier = tableInfo.getOrCreateAbsoluteTableIdentifier();

    CarbonTablePath carbonTablePath = CarbonStorePath.getCarbonTablePath(
        identifier.getTablePath(),
        identifier.getCarbonTableIdentifier());
    String schemaFilePath = carbonTablePath.getSchemaFilePath();
    String schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath);
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo);
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    org.apache.carbondata.format.TableInfo thriftTableInfo =
        schemaConverter.fromWrapperToExternalTableInfo(
            tableInfo,
            tableInfo.getDatabaseName(),
            tableInfo.getFactTable().getTableName());
    org.apache.carbondata.format.SchemaEvolutionEntry schemaEvolutionEntry =
        new SchemaEvolutionEntry(
            tableInfo.getLastUpdatedTime());
    thriftTableInfo.getFact_table().getSchema_evolution().getSchema_evolution_history()
        .add(schemaEvolutionEntry);
    FileFactory.FileType fileType = FileFactory.getFileType(schemaMetadataPath);
    if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
      FileFactory.mkdirs(schemaMetadataPath, fileType);
    }
    ThriftWriter thriftWriter = new ThriftWriter(schemaFilePath, false);
    thriftWriter.open();
    thriftWriter.write(thriftTableInfo);
    thriftWriter.close();
    CarbonTable table = CarbonMetadata.getInstance().getCarbonTable(tableInfo.getTableUniqueName());
    return new Table(table);
  }

  public TableBuilder databaseName(String databaseName) {
    this.databaseName = databaseName;
    return this;
  }

  public TableBuilder tableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public TableBuilder tableSchema(TableSchema schema) {
    if (tableName == null) {
      throw new IllegalArgumentException("set table name first");
    }
    schema.setTableName(tableName);
    this.tableSchema = schema;
    return this;
  }

  // check whether there are duplicated column
  private void validateSchema(StructType schema) throws MalformedCarbonCommandException {
    Set<String> fieldNames = new HashSet<>();
    List<StructField> fields = schema.getFields();
    for (StructField field : fields) {
      if (fieldNames.contains(field.getFieldName())) {
        throw new MalformedCarbonCommandException(
            "Duplicated column found, column name " + field.getFieldName());
      }
      fieldNames.add(field.getFieldName());
    }
  }

  public TableBuilder tablePath(String tablePath) {
    this.tablePath = tablePath;
    return this;
  }

}
