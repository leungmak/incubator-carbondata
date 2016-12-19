package org.apache.carbondata.hadoop.api;

public class Schema {
  Field[] fields;

  public Schema(Field[] fields) {
    this.fields = fields;
  }

  public Schema(String schemaString) {
    // convert schemaString to fields
  }
}
