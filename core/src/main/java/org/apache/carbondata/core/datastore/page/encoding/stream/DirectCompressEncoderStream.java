package org.apache.carbondata.core.datastore.page.encoding.stream;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.DirectCompressEncoderMeta;

public class DirectCompressEncoderStream implements EncoderStream {

  private Compressor compressor;
  private DataType dataType;
  private EncoderStream childStream;

  public DirectCompressEncoderStream(Compressor compressor, DataType dataType,
      EncoderStream childStream) {
    this.compressor = compressor;
    this.childStream = childStream;
    this.dataType = dataType;
  }

  @Override
  public void start() {
    childStream.start();
  }

  @Override
  public void write(byte value) throws IOException {
    childStream.write(value);
  }

  @Override
  public void write(short value) throws IOException {
    childStream.write(value);
  }

  @Override
  public void write(int value) throws IOException {
    childStream.write(value);
  }

  @Override
  public void write(long value) throws IOException {
    childStream.write(value);
  }

  @Override
  public byte[] end() throws IOException {
    byte[] bytes = childStream.end();
    if (compressor != null) return compressor.compressByte(bytes);
    else return bytes;
  }

  @Override
  public List<ValueEncoderMeta> getMeta() {
    List<ValueEncoderMeta> metas = childStream.getMeta();
    metas.add(new DirectCompressEncoderMeta(compressor.getName(), dataType));
    return metas;
  }
}
