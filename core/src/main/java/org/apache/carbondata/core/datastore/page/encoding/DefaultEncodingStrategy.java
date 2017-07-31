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

import java.util.List;

import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.encoding.stream.AdaptiveEncoderStream;
import org.apache.carbondata.core.datastore.page.encoding.stream.ByteArrayEncoderStream;
import org.apache.carbondata.core.datastore.page.encoding.stream.ColumnPageStreamDecoder;
import org.apache.carbondata.core.datastore.page.encoding.stream.ColumnPageStreamEncoder;
import org.apache.carbondata.core.datastore.page.encoding.stream.ComplexDimensionIndexCodec;
import org.apache.carbondata.core.datastore.page.encoding.stream.DictDimensionIndexCodec;
import org.apache.carbondata.core.datastore.page.encoding.stream.DiffEncoderStream;
import org.apache.carbondata.core.datastore.page.encoding.stream.DirectCompressDecoderStream;
import org.apache.carbondata.core.datastore.page.encoding.stream.DirectCompressEncoderStream;
import org.apache.carbondata.core.datastore.page.encoding.stream.DirectDictDimensionIndexCodec;
import org.apache.carbondata.core.datastore.page.encoding.stream.EncoderStream;
import org.apache.carbondata.core.datastore.page.encoding.stream.EncodingException;
import org.apache.carbondata.core.datastore.page.encoding.stream.HighCardDictDimensionIndexCodec;
import org.apache.carbondata.core.datastore.page.encoding.stream.RLEEncoderStream;
import org.apache.carbondata.core.datastore.page.statistics.PrimitivePageStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.metadata.encoder.AdaptiveCodecMeta;
import org.apache.carbondata.core.metadata.encoder.ColumnPageCodecMeta;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.DirectCompressEncoderMeta;
import org.apache.carbondata.core.metadata.encoder.Encoding;

/**
 * Default strategy will select encoding base on column page data type and statistics
 */
public class DefaultEncodingStrategy extends EncodingStrategy {

  private static final Compressor compressor = CompressorFactory.getInstance().getCompressor();

  private static final int THREE_BYTES_MAX = (int) Math.pow(2, 23) - 1;
  private static final int THREE_BYTES_MIN = - THREE_BYTES_MAX - 1;

  /**
   * create codec based on the page data type and statistics
   */
  @Override
  public ColumnPageStreamEncoder newEncoder(TableSpec.MeasureSpec measureSpec,
      SimpleStatsResult stats) {
    switch (stats.getDataType()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return newEncoderForIntegralType(measureSpec, stats);
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
      case BYTE_ARRAY:
        // no dictionary dimension
        return newDirectCompressEncoderStream(stats.getDataType());
      default:
        throw new RuntimeException("unsupported data type: " + stats.getDataType());
    }
  }

  /**
   * create codec based on the page data type and statistics contained by ValueEncoderMeta
   */
  public ColumnPageStreamEncoder newEncoder(
      ValueEncoderMeta meta, int scale, int precision) {
    if (meta instanceof AdaptiveCodecMeta) {
      AdaptiveCodecMeta codecMeta = (AdaptiveCodecMeta) meta;
      SimpleStatsResult stats = PrimitivePageStatsCollector.newInstance(codecMeta);
      switch (codecMeta.getSrcDataType()) {
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
          return newEncoderForIntegralType(stats);
        case FLOAT:
        case DOUBLE:
        case DECIMAL:
        case BYTE_ARRAY:
          // no dictionary dimension
          return newDirectCompressEncoderStream(stats.getDataType());
        default:
          throw new RuntimeException("unsupported data type: " + stats.getDataType());
      }
    } else {
      SimpleStatsResult stats = PrimitivePageStatsCollector.newInstance(meta);
      switch (meta.getType()) {
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
          return newEncoderForIntegralType(, stats);
        case FLOAT:
        case DOUBLE:
        case DECIMAL:
        case BYTE_ARRAY:
          // no dictionary dimension
          return newDirectCompressEncoderStream(stats.getDataType());
        default:
          throw new RuntimeException("unsupported data type: " + stats.getDataType());
      }
    }
  }

  @Override
  public ColumnPageStreamDecoder newDecoder(ValueEncoderMeta meta, int pageSize) {
    if (meta instanceof ColumnPageCodecMeta) {
      return getColumnPageDecoderV3((ColumnPageCodecMeta) meta, pageSize);
    } else {
      return getColumnPageDecoderV1V2(meta, pageSize);
    }
  }

  private ColumnPageStreamDecoder getColumnPageDecoderV3(ColumnPageCodecMeta meta,
      int pageSize) {
    Encoding encoding = meta.getEncoding();
    switch (encoding) {
      case :
      DirectCompressEncoderMeta dcem = (DirectCompressEncoderMeta) meta;
      DirectCompressDecoderStream stream = new DirectCompressDecoderStream(
          compressor, dcem.getDataType());
      return new ColumnPageStreamDecoder(stream, dcem.getDataType(), pageSize);
      break;
      case ADAPTIVE:
        AdaptiveCodecMeta aem = (AdaptiveCodecMeta) meta;
        SimpleStatsResult stats = PrimitivePageStatsCollector.newInstance(aem);
        switch (aem.getSrcDataType()) {
          case BYTE:
          case SHORT:
          case INT:
          case LONG:
            return newDecoderForIntegralType(meta, pageSize);
          case FLOAT:
          case DOUBLE:
          case DECIMAL:
          case BYTE_ARRAY:
            // no dictionary dimension
            return newDirectCompressDecoderStream(meta.getType(), pageSize);
          default:
            throw new RuntimeException("unsupported data type: " + stats.getDataType());
        }
      default:
    }
  }

  private ColumnPageStreamDecoder getColumnPageDecoderV1V2(ValueEncoderMeta meta, int pageSize) {
    SimpleStatsResult stats = PrimitivePageStatsCollector.newInstance(meta);
    switch (meta.getType()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return newDecoderForIntegralType(stats);
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
      case BYTE_ARRAY:
        // no dictionary dimension
        return newDirectCompressDecoderStream(meta.getType(), pageSize);
      default:
        throw new RuntimeException("unsupported data type: " + stats.getDataType());
    }
  }

  private DataType fitLongMinMax(long max, long min) {
    if (max <= Byte.MAX_VALUE && min >= Byte.MIN_VALUE) {
      return DataType.BYTE;
    } else if (max <= Short.MAX_VALUE && min >= Short.MIN_VALUE) {
      return DataType.SHORT;
    } else if (max <= THREE_BYTES_MAX && min >= THREE_BYTES_MIN) {
      return DataType.SHORT_INT;
    } else if (max <= Integer.MAX_VALUE && min >= Integer.MIN_VALUE) {
      return DataType.INT;
    } else {
      return DataType.LONG;
    }
  }

  private DataType fitMinMax(DataType dataType, Object max, Object min) {
    switch (dataType) {
      case BYTE:
        return fitLongMinMax((byte) max, (byte) min);
      case SHORT:
        return fitLongMinMax((short) max, (short) min);
      case INT:
        return fitLongMinMax((int) max, (int) min);
      case LONG:
        return fitLongMinMax((long) max, (long) min);
      case DOUBLE:
        return DataType.DOUBLE;
      default:
        throw new RuntimeException("internal error: " + dataType);
    }
  }

  // fit the long input value into minimum data type
  private DataType fitDelta(DataType dataType, Object max, Object min) {
    // use long data type to calculate delta to avoid overflow
    long value;
    switch (dataType) {
      case BYTE:
        value = (long)(byte) max - (long)(byte) min;
        break;
      case SHORT:
        value = (long)(short) max - (long)(short) min;
        break;
      case INT:
        value = (long)(int) max - (long)(int) min;
        break;
      case LONG:
        // TODO: add overflow detection and return delta type
        return DataType.LONG;
      default:
        throw new RuntimeException("internal error: " + dataType);
    }
    if (value <= Byte.MAX_VALUE && value >= Byte.MIN_VALUE) {
      return DataType.BYTE;
    } else if (value <= Short.MAX_VALUE && value >= Short.MIN_VALUE) {
      return DataType.SHORT;
    } else if (value <= THREE_BYTES_MAX && value >= THREE_BYTES_MIN) {
      return DataType.SHORT_INT;
    } else if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {
      return DataType.INT;
    } else {
      return DataType.LONG;
    }
  }

  private EncoderStream selectAdaptiveStream(SimpleStatsResult stats) {
    DataType srcDataType = stats.getDataType();
    DataType adaptiveDataType = fitMinMax(stats.getDataType(), stats.getMax(), stats.getMin());
    DataType deltaDataType;

    // TODO: this handling is for data compatibility, change to Override check when implementing
    // encoding override feature
    if (adaptiveDataType == DataType.LONG) {
      deltaDataType = DataType.LONG;
    } else {
      deltaDataType = fitDelta(stats.getDataType(), stats.getMax(), stats.getMin());
    }
    ByteArrayEncoderStream abs = new ByteArrayEncoderStream();
    if (Math.min(adaptiveDataType.getSizeInBytes(), deltaDataType.getSizeInBytes()) ==
        srcDataType.getSizeInBytes()) {
      // no effect to use adaptive or delta, use compression only
      return new DirectCompressEncoderStream(compressor, stats.getDataType(), abs);
    }
    ByteArrayEncoderStream bas = new ByteArrayEncoderStream();
    RLEEncoderStream rleStream = new RLEEncoderStream(bas);
    EncoderStream adaptiveStream;
    if (adaptiveDataType.getSizeInBytes() <= deltaDataType.getSizeInBytes()) {
      // choose adaptive encoding
      adaptiveStream = new AdaptiveEncoderStream(rleStream, srcDataType, adaptiveDataType);
    } else {
      // choose delta adaptive encoding
      adaptiveStream = new DiffEncoderStream(rleStream, srcDataType, deltaDataType,
          getMax(stats.getDataType(), stats.getMax()));
    }
    return adaptiveStream;
  }

  // choose between adaptive encoder or delta adaptive encoder, based on whose target data type
  // size is smaller
  private ColumnPageStreamEncoder newEncoderForIntegralType(TableSpec.MeasureSpec measureSpec,
      SimpleStatsResult stats) {
    List<Encoding> encodingList = measureSpec.getEncodings();
    if (encodingList == null) {
      EncoderStream stream = selectAdaptiveStream(stats);
      DirectCompressEncoderStream
          compressed = new DirectCompressEncoderStream(compressor, stats.getDataType(), stream);
      return new ColumnPageStreamEncoder(compressed);
    } else {
      EncoderStream stream;
      if (encodingList.size() != 1) {
        throw new EncodingException("internal error, only one encoding is supported");
      }
      Encoding encoding = encodingList.get(0);
      switch (encoding) {
        case ADAPTIVE:
          stream = selectAdaptiveStream(stats);
          break;
        default:
          throw new EncodingException("not support " + encoding + " encoding for column " +
              measureSpec.getFieldName());
      }
      return new ColumnPageStreamEncoder(
          new DirectCompressEncoderStream(compressor, stats.getDataType(), stream));
    }
  }

  @Override
  public ColumnPageStreamEncoder newEncoder(TableSpec.DimensionSpec dimensionSpec) {
    Compressor compressor = CompressorFactory.getInstance().getCompressor();
    switch (dimensionSpec.getDimensionType()) {
      case GLOBAL_DICTIONARY:
        return new DictDimensionIndexCodec(
            dimensionSpec.isInSortColumns(),
            dimensionSpec.isInSortColumns() && dimensionSpec.isDoInvertedIndex(),
            compressor);
      case DIRECT_DICTIONARY:
        return new DirectDictDimensionIndexCodec(
            dimensionSpec.isInSortColumns(),
            dimensionSpec.isInSortColumns() && dimensionSpec.isDoInvertedIndex(),
            compressor);
      case PLAIN_VALUE:
        return new HighCardDictDimensionIndexCodec(
            dimensionSpec.isInSortColumns(),
            dimensionSpec.isInSortColumns() && dimensionSpec.isDoInvertedIndex(),
            compressor);
      case COMPLEX:
        return new ComplexDimensionIndexCodec(false, false, compressor);
      default:
        throw new RuntimeException("unsupported dimension type: " +
            dimensionSpec.getDimensionType());
    }
  }

  private ColumnPageStreamEncoder newDirectCompressEncoderStream(DataType dataType) {
    ByteArrayEncoderStream stream = new ByteArrayEncoderStream();
    return new ColumnPageStreamEncoder(
        new DirectCompressEncoderStream(compressor, dataType, stream));
  }

  private ColumnPageStreamDecoder newDirectCompressDecoderStream(DataType dataType, int pageSize) {
    return new ColumnPageStreamDecoder(
        new DirectCompressDecoderStream(compressor, dataType),
        dataType,
        pageSize);
  }

  private ColumnPageStreamDecoder newDecoderForIntegralType(ColumnPageCodecMeta meta,
      int pageSize) {
    List<Encoding> encodingList = meta.getEncodingList();
    ColumnPageStreamDecoder streamDecoder;
    for (Encoding encoding : encodingList) {
        switch (encoding) {
          case DIRECT_COMPRESS:
            streamDecoder = new DirectCompressDecoderStream(compressor, encoding)
          case ADAPTIVE:
        }
    }
    DirectCompressDecoderStream compressed = new DirectCompressDecoderStream(compressor,
        meta.getType());
  }

  private ColumnPageStreamDecoder newDecoderForIntegralType(SimpleStatsResult stats) {
    DirectCompressDecoderStream compressed = new DirectCompressDecoderStream(compressor,
        stats.getDataType());

    return null;
  }
}
