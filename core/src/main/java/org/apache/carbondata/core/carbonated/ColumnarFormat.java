package org.apache.carbondata.core.carbonated;

public interface ColumnarFormat {
  SplitScanner.RawColumnChunk[] readRawColumnChunk(String[] filterColumnNames);

  Stats getStats(SplitScanner.RawColumnChunk chunk, page);

  FilterExecutor getFilterExecutor(ExpressionTree tree)
  DecodedColumnPage decodePage();

  class ValueFilterExectuor implements FilterExecutor {
    ValueFilterExectuor(ExpressionTree tree) {

    }
  }
}
