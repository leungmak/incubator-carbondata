package org.apache.carbondata.core.carbonated;

import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.scanner.impl.BlockletFullScanner;

import static org.apache.carbondata.core.statusmanager.FileFormat.COLUMNAR_V3;

public class FullReader<T> implements BlockReader<T> {

  BlockletFullScanner scanner;

  @Override
  public void init(
      SplitInfo splitInfo,
      String[] projectionFields,
      Expression filterExpression) {
    switch (splitInfo.format) {
      case COLUMNAR_V3:
        reader = new CarbonFileReader();
        break;
      case ROW_V1:
        reader = new CarbonStreamRecordReader();
        break;
      case CSV:
        reader = new CSVReader();
        break;
      case Parquet:
        reader = new ParquetReader();
        break;
      case HFile:
        reader = new HFileReader();
        break;
    }
  }

  @Override public boolean hasNext() {
    return reader.hasNext();
  }

  @Override public T next() {
    return reader.next();
  }

  @Override public void close() {
    reader.close();
  }
}
