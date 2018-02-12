package org.apache.carbondata.core.carbonated;

import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.scanner.impl.BlockletFilterScanner;

public class FilterReader<T> implements BlockReader<T> {

  BlockletFilterScanner scanner;

  @Override
  public void init(SplitInfo splitInfo, String[] fieldNames, Expression filterExpression) {

  }

  @Override public boolean hasNext() {
    return false;
  }

  @Override public T next() {
    return null;
  }

  @Override public void close() {

  }
}
