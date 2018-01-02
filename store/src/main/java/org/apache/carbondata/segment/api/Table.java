package org.apache.carbondata.segment.api;

import java.io.IOException;
import java.util.HashMap;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.hadoop.api.CarbonOutputCommitter;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;
import org.apache.carbondata.spark.util.DataLoadingUtil;

public class Table {

  private CarbonTable table;

  Table(CarbonTable table) {
    this.table = table;
  }

  public CarbonLoadModel openNewSegment() throws IOException {
    CarbonLoadModel loadModel = DataLoadingUtil.buildCarbonLoadModelJava(
        table, new HashMap<String, String>());
    CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(loadModel, false);
    return loadModel;
  }

  public void commitSegment(CarbonLoadModel loadModel) throws IOException {
    CarbonOutputCommitter.commitSegment(loadModel, false, null);
  }
}
