package org.apache.carbondata.core.carbonated;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;

public abstract class SplitScanner {

  RawColumnChunk[] rawColumnPages;

  void addColumnPage(RawColumnChunk)

  abstract class AbstractRawColumnChunk {
    ByteBuffer buffer;
  }

  abstract class RawColumnChunk extends AbstractRawColumnChunk { }

  public interface RawColumnPageReader {
    RawColumnChunk readRawPage(CarbonFile file, int columnIndexStart,
        int columnIndexEnd);
  }

  public interface RawColumnPageDecoder {
    ColumnPage decodePage(RawColumnChunk page);
  }

  public void scan(String[] filterColumnNames, String[] projectColumnNames, Expression filterExpression) {

    if (columnar) {
      ColumnarFormat format;
      FilterMatcher filterMatcher = format.getFilterExecutor(tree);
      RawColumnChunk[] filterChunks = format.readRawColumnChunk(filterColumnNames);
      while (filterChunk.hasNextPage()) {
        RawColumnPage[] rawPages = filterChunk.getNextPage();
        Stats stats = format.getStats(page);
        boolean hit = filterMatcher.isHit(stats);
        if (hit) {
          //DecodedColumnPage[] decodedFilterPages = lowLevelReader.decodePage(rawPages);
          BitSet (hitRows, decodedPage) = filterMatcher.filter(rawPages, format);
          if (!hitRows.isEmpty()) {
            DecodedColumnPage[] decodedProjectPages
            if (projection has more column){
              decodedProjectPages = lowLevelReader.readAndDecodePage(projectColumnNames, page.getPageId());
            }
            resultStore.add(hitRows, decodedFilterPages, decodedProjectPages);
          }
        }
      }
    } else {
      while (lowLevelReader.hasNextPage()) {
        Page page = lowLevelReader.getNextPage()
//        DecodedColumnPage[] allPages =
//            lowLevelReader.readAndDecodePages(filterColumnNames, projectColumnNames, pageId);
        BitSet hitRows = FilterMatcher.filter(filterExpression, format);
        if (!hitRows.isEmpty()) {
          resultStore.add(hitRows, decodedFilterPages, decodedProjectPages);
        }
      }
    }
  }

}
