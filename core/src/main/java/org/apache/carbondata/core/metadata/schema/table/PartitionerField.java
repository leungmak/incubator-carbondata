package org.apache.carbondata.core.metadata.schema.table;

import org.apache.carbondata.core.metadata.datatype.DataType;

public class PartitionerField {
  private String partitionColumn;
  private DataType dataType;
  private String columnComment;

  public PartitionerField(String partitionColumn, DataType dataType, String columnComment) {
    this.partitionColumn = partitionColumn;
    this.dataType = dataType;
    this.columnComment = columnComment;
  }

  public String getPartitionColumn() {
    return partitionColumn;
  }

  public DataType getDataType() {
    return dataType;
  }

  public String getColumnComment() {
    return columnComment;
  }
}
