package org.apache.carbondata.core.datastore.page.encoding.stream;

import java.io.IOException;

class RLEDecoderStream implements DecoderStream {
  private DecoderStream child;

  public RLEDecoderStream(DecoderStream child) {
    this.child = child;
  }

  @Override
  public void start() {

  }

  @Override
  public byte readByte() throws IOException {
    return 0;
  }

  @Override
  public short writeShort() throws IOException {
    return 0;
  }

  @Override
  public int writeInt() throws IOException {
    return 0;
  }

  @Override
  public long writeLong() throws IOException {
    return 0;
  }

  @Override
  public byte[] end() throws IOException {
    return new byte[0];
  }
}