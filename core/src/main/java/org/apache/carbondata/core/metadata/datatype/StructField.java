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
import java.util.HashMap;
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
   * @param tableProperty table property
   * @param parentTable parent table of this field, can be null
   * @param dataMapFields datamap on this field, can be null
   * @return ColumnSchema list
   */
  public List<ColumnSchema> createColumnSchema(
      TableProperty tableProperty,
      CarbonTable parentTable,
      Map<String, DataMapField> dataMapFields) {
    List<ColumnSchema> columnSchemas = new ArrayList<>();

    if (!dataType.isComplexType()) {
      ColumnSchema columnSchema = createPrivitiveColumnSchema(tableProperty, parentTable, dataMapFields);
      columnSchemas.add(columnSchema);
    } else if (DataTypes.isStructType(dataType)) {
      List<StructField> fields = ((StructType) dataType).getFields();
      for (StructField field : fields) {
        List<ColumnSchema> columns = field.createColumnSchema(tableProperty, parentTable, dataMapFields);
        columnSchemas.addAll(columns);
      }
    } else if (DataTypes.isArrayType(dataType)) {
      ColumnSchema columnSchema = new ColumnSchema();
      columnSchema.setDataType(dataType);
      columnSchema.setColumnName(fieldName);
      columnSchema.setEncodingList(getEncodingsForField(tableProperty, parentTable, dataMapFields));
      String columnUniqueId =
          CarbonCommonFactory.getColumnUniqueIdGenerator().generateUniqueId(columnSchema);
      columnSchema.setColumnUniqueId(columnUniqueId);
      columnSchema.setColumnReferenceId(columnUniqueId);
      columnSchema.setDimensionColumn(isDimension(tableProperty));
      columnSchema.setSortColumn(isSortColumn(tableProperty));
      columnSchema.setUseInvertedIndex(isInvertedIndexColumn(tableProperty));
      columnSchema.setPrecision(dataType);
      columnSchema.setScale(dataType);
      columnSchema.setSchemaOrdinal(schemaOrdinal);
      columnSchema.setNumberOfChild(dataType.getNumOfChild());
      columnSchema.setInvisible(false);
      columnSchema.setColumnar(true);
      columnSchema.setColumnGroup(-1);
      columnSchema.setColumnProperties(new HashMap<String, String>());
      columnSchemas.add(columnSchema);

      DataType elementType = ((ArrayType) dataType).getElementType();
      StructField valueField = DataTypes.createStructField(fieldName + ".val", elementType);
      List<ColumnSchema> valueColumns = valueField.createColumnSchema(tableProperty, parentTable, dataMapFields);
      valueColumns.get(0).setEncodingList(columnSchema.getEncodingList());
      columnSchemas.addAll(valueColumns);
    } else if (DataTypes.isMapType(dataType)) {
      // TODO
    } else {
      throw new UnsupportedOperationException("unsupported type: " + dataType);
    }

    return columnSchemas;
  }

  /**
   * Create ColumnSchema object represent for privite field
   *
   * @param tableProperty table property
   * @param parentTable parent table of this field, can be null
   * @param dataMapFields datamap on this field, can be null
   * @return ColumnSchema
   */
  public ColumnSchema createPrivitiveColumnSchema(
      TableProperty tableProperty,
      CarbonTable parentTable,
      Map<String, DataMapField> dataMapFields) {
    ColumnSchema columnSchema = new ColumnSchema();
    columnSchema.setDataType(dataType);
    columnSchema.setColumnName(fieldName);
    columnSchema.setEncodingList(getEncodingsForField(tableProperty, parentTable, dataMapFields));
    String columnUniqueId =
        CarbonCommonFactory.getColumnUniqueIdGenerator().generateUniqueId(columnSchema);
    columnSchema.setColumnUniqueId(columnUniqueId);
    columnSchema.setColumnReferenceId(columnUniqueId);
    columnSchema.setDimensionColumn(isDimension(tableProperty));
    columnSchema.setSortColumn(isSortColumn(tableProperty));
    columnSchema.setUseInvertedIndex(isInvertedIndexColumn(tableProperty));
    columnSchema.setPrecision(dataType);
    columnSchema.setScale(dataType);
    columnSchema.setSchemaOrdinal(schemaOrdinal);
    columnSchema.setNumberOfChild(dataType.getNumOfChild());
    columnSchema.setInvisible(false);
    columnSchema.setDataType(dataType);
    columnSchema.setColumnar(true);
    columnSchema.setColumnGroup(-1);
    columnSchema.setColumnProperties(new HashMap<String, String>());

    boolean hasDataMap = dataMapFields != null && dataMapFields.get(fieldName) != null;
    if (hasDataMap) {
      DataMapField dataMapField = dataMapFields.get(fieldName);
      columnSchema.setAggFunction(dataMapField.getAggregateFunction());
      ColumnTableRelation relation = dataMapField.getColumnTableRelation();
      List<ParentColumnTableRelation> parentColumnTableRelationList = new ArrayList<>();
      RelationIdentifier relationIdentifier =
          new RelationIdentifier(
              relation.getParentDatabaseName(),
              relation.getParentTableName(),
              relation.getParentTableId());
      ParentColumnTableRelation parentColumnTableRelation =
          new ParentColumnTableRelation(
              relationIdentifier,
              relation.getParentColumnId(),
              relation.getParentColumnName());
      parentColumnTableRelationList.add(parentColumnTableRelation);
      columnSchema.setParentColumnTableRelations(parentColumnTableRelationList);
    }
    return columnSchema;
  }

  /**
   * Return true if this field is dimension
   */
  public boolean isDimension(TableProperty tableProperty) {
    boolean isDimension = false;
    String fieldName = this.fieldName.toLowerCase();
    if (tableProperty.getSortColumns().contains(fieldName)) {
      isDimension = true;
    }
    if (dataType == DataTypes.DATE || dataType == DataTypes.TIMESTAMP) {
      isDimension = true;
    } else if (dataType == DataTypes.STRING || dataType.isComplexType()) {
      isDimension = true;
    } else if (tableProperty.getDictionaryColumns().contains(fieldName)) {
      isDimension = true;
    }
    return isDimension;
  }

  /**
   * Return true if this field is in sort columns
   */
  public boolean isSortColumn(TableProperty tableProperty) {
    String fieldName = this.fieldName.toLowerCase();
    return tableProperty.getSortColumns().contains(fieldName);
  }

  /**
   * Return true if this field use inverted index
   */
  public boolean isInvertedIndexColumn(TableProperty tableProperty) {
    if (dataType.isComplexType()) {
      return true;
    } else {
      String fieldName = this.fieldName.toLowerCase();
      return tableProperty.getSortColumns().contains(fieldName) &&
          !tableProperty.getNoInvertedIndexColumns().contains(fieldName);
    }
  }

  private List<Encoding> getEncodingsForField(TableProperty tableProperty, CarbonTable parentTable,
      Map<String, DataMapField> dataMapFields) {
    String fieldName = this.fieldName.toLowerCase();
    boolean inSortColumn = isSortColumn(tableProperty);
    boolean useInvertedIndex = isInvertedIndexColumn(tableProperty);
    boolean useDictionary = tableProperty.getDictionaryColumns().contains(fieldName);

    List<Encoding> encodings = new ArrayList<>();
    if (inSortColumn) {
      // if this field datamap field, use encoder from parent table,
      // otherwise use no dictionary (means encoding should be empty)
      if (parentTable != null && dataMapFields.containsKey(fieldName)) {
        encodings = parentTable.getColumnByName(
            parentTable.getTableName(),
            dataMapFields.get(fieldName).getColumnTableRelation().getParentColumnName()
        ).getEncoder();
      }
    }

    if (useInvertedIndex) {
      encodings.add(Encoding.INVERTED_INDEX);
    }
    if (dataType.isComplexType()) {
      encodings.add(Encoding.DICTIONARY);
    } else if (dataType == DataTypes.DATE || dataType == DataTypes.TIMESTAMP) {
      encodings.add(Encoding.DIRECT_DICTIONARY);
      encodings.add(Encoding.DICTIONARY);
    } else if (useDictionary) {
      encodings.add(Encoding.DICTIONARY);
    }
    return encodings;
  }

}
