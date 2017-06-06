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

package org.apache.carbondata.core.datastore.page.encoding;

import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.ColumnPageTransform;
import org.apache.carbondata.core.datastore.page.LazyColumnPage;
import org.apache.carbondata.core.datastore.page.statistics.ColumnPageStatsVO;
import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * Codec for integer (byte, short, int, long) data type page.
 * This codec will calculate delta of page max value and page value,
 * and do type casting of the diff to make storage minimum.
 */
public class DeltaIntegerCodec extends ColumnPageCodec {

  public static DeltaIntegerCodec newInstance(DataType srcDataType, DataType targetDataType,
      ColumnPageStatsVO stats) {
    return new DeltaIntegerCodec(srcDataType, targetDataType, stats);
  }

  private DeltaIntegerCodec(DataType srcDataType, DataType targetDataType,
      ColumnPageStatsVO stats) {
    super(srcDataType, targetDataType, stats);
  }
  @Override
  public String getName() {
    return "DeltaIntegerCodec";
  }

  @Override
  public byte[] encode(ColumnPage input) {
    input.transformAndCastTo(
        ColumnPageTransform.MAX_DELTA, input.getStatistics().getDecimal(), targetDataType);
    return input.compress(compressor);
  }

  @Override
  public ColumnPage decode(byte[] input, int offset, int length) {
    ColumnPage page = ColumnPage.decompress(compressor, targetDataType, input, offset, length);
    return LazyColumnPage.newPage(page, ColumnPageTransform.MAX_DELTA, stats);
  }
}
