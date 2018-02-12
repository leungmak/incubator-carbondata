package org.apache.carbondata.core.carbonated;

import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.datatype.StructType;

public interface SplitReader {

  void init(StructType schema, String path, int offset, int length);

  void readPage(String[] fieldNames, ColumnPage[] outputPages);

  void close();
}
