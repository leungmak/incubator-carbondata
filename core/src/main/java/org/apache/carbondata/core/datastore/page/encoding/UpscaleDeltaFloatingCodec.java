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
 * Codec for floating point (float, double) data type page.
 * This codec will upscale (multiple page value by decimal) to integer value,
 * and do type casting to make storage minimum.
 */
public class UpscaleDeltaFloatingCodec extends ColumnPageCodec {

  // TODO: cache and reuse the same encoder since snappy is thread-safe

  public static ColumnPageCodec newInstance(DataType srcDataType, DataType targetDataType,
      ColumnPageStatsVO stats) {
    return new UpscaleDeltaFloatingCodec(srcDataType, targetDataType, stats);
  }

  private UpscaleDeltaFloatingCodec(DataType srcDataType, DataType targetDataType,
      ColumnPageStatsVO stats) {
    super(srcDataType, targetDataType, stats);
  }

  @Override
  public String getName() {
    return "UpscaleDeltaFloatingCodec";
  }

  @Override
  public byte[] encode(ColumnPage input) {
    input.transformAndCastTo(
        ColumnPageTransform.UPSCALE_MAX_DELTA, stats.getDecimal(), targetDataType);
    return input.compress(compressor);
  }

  @Override
  public ColumnPage decode(byte[] input, int offset, int length) {
    ColumnPage page = ColumnPage.decompress(compressor, targetDataType, input, offset, length);
    return LazyColumnPage.newPage(page, ColumnPageTransform.UPSCALE_MAX_DELTA, stats);
  }
}