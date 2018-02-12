package org.apache.carbondata.core.carbonated.impl;

import org.apache.carbondata.core.carbonated.SplitReader;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.datatype.StructType;

public class CSVSplitReader implements SplitReader {
  @Override
  public void init(StructType schema, String path, int offset, int length) {

  }

  @Override
  public void readPage(String[] fieldNames, ColumnPage[] outputPages) {

  }

  @Override
  public void close() {

  }
}
