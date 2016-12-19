package org.apache.carbondata.spark;

import org.apache.carbondata.hadoop.api.CarbonTableWriteSupport;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.spark.sql.Row;

public class SparkRowWriteSupport extends CarbonTableWriteSupport<Row> {
  @Override public CarbonRow convert(Row data) {
    return null;
  }
}
