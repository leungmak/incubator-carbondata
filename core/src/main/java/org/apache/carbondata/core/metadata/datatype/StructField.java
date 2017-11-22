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

package org.apache.carbondata.core.metadata.datatype;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.TableProperty;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.ColumnTableRelation;
import org.apache.carbondata.core.metadata.schema.table.DataMapField;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.metadata.schema.table.column.ParentColumnTableRelation;
import org.apache.carbondata.core.service.CarbonCommonFactory;

public class StructField implements Serializable {

  private static final long serialVersionUID = 3271726L;

  private String fieldName;

  private DataType dataType;

  // ordinal in the struct type schema
  private int schemaOrdinal;

  private String fieldComment;

  /**
   * Use DataTypes.createStructField to create instance
   */
  StructField(String fieldName, DataType dataType) {
    this.fieldName = fieldName;
    this.dataType = dataType;
  }

  public DataType getDataType() {
    return dataType;
  }

  public String getFieldName() {
    return fieldName;
  }

  public int getSchemaOrdinal() {
    return schemaOrdinal;
  }

  public void setSchemaOrdinal(int schemaOrdinal) {
    this.schemaOrdinal = schemaOrdinal;
  }

  public String getFieldComment() {
    return fieldComment;
  }

  public void setFieldComment(String fieldComment) {
    this.fieldComment = fieldComment;
  }

  /**
   * Create ColumnSchema object represent for this field, it will be multiple ColumnSchema objects
   * if it is a complex type
   *
   * @param sortColumns column name list in sort_columns property
   * @param parentTable parent table of this field, can be null
   * @param dataMapFields datamap on this field, can be null
   * @return ColumnSchema list
   */
  public ColumnSchema createColumnSchema(
      List<String> sortColumns,
      Map<String, String> tableProperties,
      CarbonTable parentTable,
      Map<String, DataMapField> dataMapFields) {
    boolean isDimension = false;
    List<Encoding> encodings = new ArrayList<>();
    List<String> noInvertedIndexColumns = TableProperty.getNoInvertedIndexColumns(tableProperties);
    List<String> dictionaryColumns = TableProperty.getDictionaryColumns(tableProperties);
    String fieldName = this.fieldName.toLowerCase();
    boolean inSortColumn = sortColumns.contains(fieldName);
    boolean useInvertedIndex = sortColumns.contains(fieldName) && !noInvertedIndexColumns.contains(fieldName);
    boolean useDictionary = dictionaryColumns.contains(fieldName);
    boolean hasDataMap = dataMapFields != null && dataMapFields.get(fieldName) != null;

    if (inSortColumn) {
      // if this field datamap field, use encoder from parent table,
      // otherwise use no dictionary (means encoding should be empty)
      if (parentTable != null && dataMapFields.containsKey(fieldName)) {
        encodings = parentTable.getColumnByName(
            parentTable.getTableName(),
            dataMapFields.get(fieldName).getColumnTableRelation().getParentColumnName()
        ).getEncoder();
      }
      isDimension = true;
    }

    if (dataType == DataTypes.DATE || dataType == DataTypes.TIMESTAMP) {
      encodings.add(Encoding.DIRECT_DICTIONARY);
      encodings.add(Encoding.DICTIONARY);
      isDimension = true;
    } else if (dataType == DataTypes.STRING || dataType.isComplexType()) {
      isDimension = true;
    } else if (useDictionary) {
      encodings.add(Encoding.DICTIONARY);
      isDimension = true;
    }

    if (useInvertedIndex) {
      encodings.add(Encoding.INVERTED_INDEX);
    }

    ColumnSchema columnSchema = new ColumnSchema();
    columnSchema.setDataType(dataType);
    columnSchema.setColumnName(fieldName);
    columnSchema.setEncodingList(encodings);
    String columnUniqueId = CarbonCommonFactory.getColumnUniqueIdGenerator().generateUniqueId(columnSchema);
    columnSchema.setColumnUniqueId(columnUniqueId);
    columnSchema.setColumnReferenceId(columnUniqueId);
    columnSchema.setDimensionColumn(isDimension);
    columnSchema.setSortColumn(inSortColumn);
    columnSchema.setUseInvertedIndex(useInvertedIndex);
    columnSchema.setPrecision(dataType);
    columnSchema.setScale(dataType);
    columnSchema.setSchemaOrdinal(schemaOrdinal);
    columnSchema.setNumberOfChild(dataType.getNumOfChild());
    columnSchema.setInvisible(false);
    columnSchema.setDataType(dataType);
    columnSchema.setColumnName(fieldName);
    columnSchema.setColumnar(true);
    columnSchema.setColumnGroup(-1);
    columnSchema.setDefaultValue(null);
    columnSchema.setAggFunction(null);
    columnSchema.setParentColumnTableRelations(null);
    columnSchema.setColumnProperties(null);

    if (hasDataMap) {
      DataMapField dataMapField = dataMapFields.get(fieldName);
      columnSchema.setAggFunction(dataMapField.getAggregateFunction());
      ColumnTableRelation relation = dataMapField.getColumnTableRelation();
      List<ParentColumnTableRelation> parentColumnTableRelationList = new ArrayList<>();
      RelationIdentifier relationIdentifier =
          new RelationIdentifier(relation.getParentDatabaseName(), relation.getParentTableName(),
              relation.getParentTableId());
      ParentColumnTableRelation parentColumnTableRelation =
          new ParentColumnTableRelation(relationIdentifier, relation.getParentColumnId(), relation.getParentColumnName());
      parentColumnTableRelationList.add(parentColumnTableRelation);
      columnSchema.setParentColumnTableRelations(parentColumnTableRelationList);
    }
    return columnSchema;
  }

  /**
   * return true if it is dimension data type
   */
  public boolean isDimension(Map<String, String> tableProperties) {
    String dictInclude = tableProperties.get(CarbonCommonConstants.DICTIONARY_INCLUDE);
    if (dictInclude != null) {
      String[] dictIncludeCols = dictInclude.split(",");
      if (Arrays.asList(dictIncludeCols).indexOf(fieldName) != -1) {
        return true;
      }
    }
    return dataType == DataTypes.STRING || dataType.isComplexType() ||
        dataType == DataTypes.TIMESTAMP || dataType == DataTypes.DATE;
  }

}
