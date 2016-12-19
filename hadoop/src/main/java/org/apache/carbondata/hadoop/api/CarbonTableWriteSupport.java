package org.apache.carbondata.hadoop.api;

import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.hadoop.conf.Configuration;

public abstract class CarbonTableWriteSupport<T> {

  private static String WRITE_SUPPORT = "carbon.write.support";

  private Schema schema;

  public static void setSchema(Configuration conf, Schema schema) {
    if (schema != null) {
      conf.set(WRITE_SUPPORT, schema.toString());
    }
  }

  public void initialize(Configuration conf) {
    String schemaString = conf.get(WRITE_SUPPORT);
    schema = new Schema(schemaString);
  }

  public abstract CarbonRow convert(T data);
}
