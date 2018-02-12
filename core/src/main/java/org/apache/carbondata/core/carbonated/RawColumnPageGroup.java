package org.apache.carbondata.core.carbonated;

import java.nio.ByteBuffer;

import org.apache.carbondata.core.datastore.filesystem.CarbonFile;

public abstract class RawColumnPageGroup {

  RawColumnPage[] dimensionPages;
  RawColumnPage[] measurePages;

  abstract class RawColumnPage {
    ByteBuffer buffer;

  }

  public abstract class RawReader {
    abstract RawColumnPage readRawPage(CarbonFile file, )
  }

}
