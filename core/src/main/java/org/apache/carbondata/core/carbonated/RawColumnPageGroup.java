package org.apache.carbondata.core.carbonated;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.scan.expression.Expression;

public abstract class RawColumnPageGroup {

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

  public void scan(String filterColumnName, String[] projectColumnNames, Expression filterExpression) {
    if (columnar) {
      RawColumnChunk filterChunk = lowLevelReader.readRawColumnChunk(filterColumnName);
      while (filterChunk.hasNextPage()) {
        RawColumnPage page = filterChunk.getNextPage();
        boolean hit = FilterMatcher.isHit(page, filterExpression);
        if (hit) {
          DecodedColumnPage decodedFilterPage = lowLevelReader.decodePage(page);
          BitSet hitRows = FilterMatcher.filter(decodedFilterPage, filterExpression);
          if (!hitRows.isEmpty()) {
            DecodedColumnPage[] decodedProjectPages =
                lowLevelReader.readAndDecodePage(projectColumnNames, page.getPageId());
            resultStore.add(hitRows, decodedFilterPage, decodedProjectPages);
          }
        }
      }
    } else {
      while (lowLevelReader.hasNextPage()) {
        DecodedColumnPage[] allPages =
            lowLevelReader.readAndDecodePages(filterColumnName, projectColumnNames, pageId);
        BitSet hitRows = FilterMatcher.filter(decodedFilterPage, filterExpression);
        if (!hitRows.isEmpty()) {
          resultStore.add(hitRows, decodedFilterPage, decodedProjectPages);
        }
      }
    }
  }

}
