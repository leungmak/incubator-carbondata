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

import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.page.encoding.stream.ColumnPageStreamDecoder;
import org.apache.carbondata.core.datastore.page.encoding.stream.ColumnPageStreamEncoder;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;

/**
 * Base class for encoding strategy implementation.
 */
public abstract class EncodingStrategy {

  // for byte array
  public abstract ColumnPageStreamEncoder newEncoder(TableSpec.MeasureSpec measureSpec,
      SimpleStatsResult stats);

  // for dimension column
  public abstract ColumnPageStreamEncoder newEncoder(TableSpec.DimensionSpec dimensionSpec);


  /**
   * decode byte array from offset to a column page
   * @param input encoded byte array
   * @param offset startoffset of the input to decode
   * @param length length of data to decode
   * @return decoded data
   */
  public abstract ColumnPageStreamDecoder newDecoder(ValueEncoderMeta meta, int pageSize);

}
