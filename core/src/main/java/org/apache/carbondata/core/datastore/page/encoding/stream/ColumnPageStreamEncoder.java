/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.datastore.page.encoding.stream;

import java.io.IOException;

import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.EncodedMeasurePage;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;

/**
 *  Codec for a column page data, implementation should not keep state across pages,
 *  caller may use the same object to apply multiple pages.
 */
public class ColumnPageStreamEncoder {

  private EncoderStream encoderStream;

  public ColumnPageStreamEncoder(EncoderStream stream) {
    this.encoderStream = stream;
  }

  /**
   * encode a column page and return the encoded data
   */
  public EncodedColumnPage encode(ColumnPage input) throws IOException {
    SimpleStatsResult stats = (SimpleStatsResult)input.getStatistics();
//    ValueEncoderMeta metadata = CodecMetaFactory.createMeta(stats);
    encoderStream.start();
    switch (input.getDataType()) {
      case BYTE:
        byte[] bytePage = input.getBytePage();
        for (int i = 0; i < bytePage.length; i++) {
          encoderStream.write(bytePage[i]);
        }
        break;
      case SHORT:
        short[] shortPage = input.getShortPage();
        for (int i = 0; i < shortPage.length; i++) {
          encoderStream.write(shortPage[i]);
        }
        break;
      case INT:
        int[] intPage = input.getIntPage();
        for (int i = 0; i < intPage.length; i++) {
          encoderStream.write(intPage[i]);
        }
        break;
      case LONG:
        long[] longPage = input.getLongPage();
        for (int i = 0; i < longPage.length; i++) {
          encoderStream.write(longPage[i]);
        }
        break;
      default:
        throw new UnsupportedOperationException();
    }
    byte[] bytes = encoderStream.end();
    return new EncodedMeasurePage(
        input.getPageSize(),
        bytes,
        encoderStream.getMeta(),
        stats.getNullBits());
  }

}
