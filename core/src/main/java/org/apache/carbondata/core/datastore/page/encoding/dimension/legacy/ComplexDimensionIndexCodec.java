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

package org.apache.carbondata.core.datastore.page.encoding.dimension.legacy;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.Encoding;

public class ComplexDimensionIndexCodec extends IndexStorageCodec {

  public ComplexDimensionIndexCodec(boolean isSort, boolean isInvertedIndex, Compressor compressor) {
    super(isSort, isInvertedIndex, compressor);
  }

  @Override
  public String getName() {
    return "ComplexDimensionIndexCodec";
  }

  @Override
  public ColumnPageEncoder createEncoder(Map<String, String> parameter) {
    return new ColumnPageEncoder() {
      @Override
      protected byte[] encodeData(ColumnPage input) throws MemoryException, IOException {
        return new byte[0];
      }

      @Override
      protected List<Encoding> getEncodingList() {
        return null;
      }

      @Override
      protected ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage) {
        return null;
      }

      @Override
      protected void fillLegacyFields(ColumnPage inputPage, DataChunk2 dataChunk)
          throws IOException {

      }

    };
  }

}
