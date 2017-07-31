package org.apache.carbondata.core.datastore.page.encoding.stream;

import java.io.IOException;

import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.AdaptiveCodecMeta;
import org.apache.carbondata.core.metadata.encoder.CodecStreamMeta;

public class AdaptiveEncoderStream implements EncoderStream {

  private EncoderStream childStream;
  private DataType srcDataType;
  private DataType targetDataType;

  public AdaptiveEncoderStream(EncoderStream childStream, DataType srcDataType,
      DataType targetDatatype) {
    this.childStream = childStream;
    this.srcDataType = srcDataType;
    this.targetDataType = targetDatatype;
  }

  @Override
  public void start() {
    childStream.start();
  }

  @Override
  public void write(byte value) throws IOException {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void write(short value) throws IOException {
    assert targetDataType == DataType.BYTE;
    childStream.write((byte) value);
  }

  @Override
  public void write(int value) throws IOException {
    assert targetDataType == DataType.BYTE || targetDataType == DataType.SHORT;
    if (targetDataType == DataType.BYTE) {
      childStream.write((byte) value);
    } else {
      childStream.write((short) value);
    }
  }

  @Override
  public void write(long value) throws IOException {
    assert targetDataType == DataType.BYTE || targetDataType == DataType.SHORT ||
        targetDataType == DataType.INT;
    if (targetDataType == DataType.BYTE) {
      childStream.write((byte) value);
    } else if (targetDataType == DataType.SHORT){
      childStream.write((short) value);
    } else {
      childStream.write((int) value);
    }
  }

  @Override
  public byte[] end() throws IOException {
    return childStream.end();
  }

  @Override
  public CodecStreamMeta getMeta() {
    return new AdaptiveCodecMeta(srcDataType, targetDataType);
  }

}
