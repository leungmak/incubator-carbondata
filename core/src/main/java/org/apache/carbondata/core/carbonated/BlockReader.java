package org.apache.carbondata.core.carbonated;

import org.apache.carbondata.core.metadata.datatype.StructType;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.statusmanager.FileFormat;

public interface BlockReader<T> {

  void init(
      SplitInfo splitInfo,
      String[] fieldNames,
      Expression filterExpression);

  boolean hasNext();

  T next();

  void close();

  class SplitInfo {
    String path;
    int offset;
    int length;
    FileFormat format;
    StructType schema;
  }
}
