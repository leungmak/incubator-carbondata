package org.apache.carbondata.core.datastore.page.encoding.stream;

import java.io.IOException;

import org.apache.carbondata.core.metadata.datatype.DataType;

public class DiffEncoderStream implements EncoderStream {

  private EncoderStream childStream;
  private DataType targetDataType;
  private long max;

  public DiffEncoderStream(EncoderStream childStream, DataType targetDatatype, long max) {
    this.childStream = childStream;
    this.targetDataType = targetDatatype;
    this.max = max;
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
    childStream.write((byte) (max - value));
  }

  @Override
  public void write(int value) throws IOException {
    assert targetDataType == DataType.BYTE || targetDataType == DataType.SHORT;
    if (targetDataType == DataType.BYTE) {
      childStream.write((byte) (max - value));
    } else {
      childStream.write((short) (max - value));
    }
  }

  @Override
  public void write(long value) throws IOException {
    assert targetDataType == DataType.BYTE || targetDataType == DataType.SHORT ||
        targetDataType == DataType.INT;
    if (targetDataType == DataType.BYTE) {
      childStream.write((byte) (max - value));
    } else if (targetDataType == DataType.SHORT){
      childStream.write((short) (max - value));
    } else {
      childStream.write((int) (max - value));
    }
  }

  @Override
  public byte[] end() throws IOException {
    return childStream.end();
  }
}
