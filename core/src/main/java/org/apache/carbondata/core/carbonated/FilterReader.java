package org.apache.carbondata.core.carbonated;

import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.scanner.impl.BlockletFilterScanner;

public class FilterReader<T> implements BlockReader<T> {

  BlockletFilterScanner scanner;
  SplitReader reader;

  @Override
  public void init(SplitInfo splitInfo, String[] fieldNames, Expression filterExpression) {

  }

  @Override public boolean hasNext() {
    return false;
  }

  @Override public T next() {

    /*

    logic of SplitReader.scan()
    1. read raw data into a raw column page, with configured size
      if (columnar) {
        for columnar format, read filter column first into raw page in memory
      } else {
        for raw format, read projection and filter column in one shot
      }
    2. check whether this page satisfied filter condition
      if (columnar) {
        for columnar format, check statistics of the page,
        decide whether it satisfies filter.
        if (satisfied) {
          decode this page and read projection column for this page
        }
      } else {
        for row format, the raw column page actually is a decoded page,
        otherwise it can not be converted to columnar data
      }
    3. loop step 1-2 until split is finished

     */
    return null;
  }

  @Override public void close() {

  }
}
