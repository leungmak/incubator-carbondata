package org.apache.carbondata.core.datastore.page.encoding;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.datastore.page.PrimitiveCodec;
import org.apache.carbondata.core.metadata.datatype.DataType;

// RLE encode/decode implementation
public class RLEEncoding implements PrimitiveCodec {

  enum RUN_STATE { INITIALIZING, REPEATED_RUN, NONREPEATED_RUN}

  // While encoding RLE, this class internally work as a state machine
  // INITIALIZING state is the initial state for each run
  // REPEATED_RUN state means it is collecting repeated values (`lastValue`)
  // NONREPEATED_RUN state means it is collecting non-repeated values (`nonRepeatValues`)
  private RUN_STATE runState = RUN_STATE.INITIALIZING;

  // count for each run, either REPEATED_RUN or NONREPEATED_RUN
  private int valueCount = 0;

  // collected value for REPEATED_RUN
  private Object lastValue;

  // collected value for NONREPEATED_RUN
  private List<Object> nonRepeatValues = new ArrayList<>();

  private ByteArrayOutputStream bao;
  private DataOutputStream encodedStream;

  private DataType dataType;
  private int inputPageSize;

  private RLEEncoding() {
  }

  static RLEEncoding newEncoder(int inputPageSize, DataType dataType) {
    switch (dataType) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        break;
      default:
        throw new UnsupportedOperationException("RLE is only supported for integral type");
    }
    RLEEncoding encoding = new RLEEncoding();
    encoding.inputPageSize = inputPageSize;
    encoding.dataType = dataType;
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    encoding.bao = bao;
    encoding.encodedStream = new DataOutputStream(bao);
    return encoding;
  }

  private void writeObject(Object value) throws IOException {
    switch (dataType) {
      case BYTE:
        encodedStream.writeByte((byte) value);
        break;
      case SHORT:
        encodedStream.writeShort((short) value);
        break;
      case INT:
        encodedStream.writeInt((int) value);
        break;
      case LONG:
        encodedStream.writeLong((long) value);
        break;
      default:
        throw new RuntimeException("internal error");
    }
  }

  // when last row is reached for encoding, write out all collected data
  private void onEncodingLastRow(int rowId) throws IOException {
    switch (runState) {
      case REPEATED_RUN:
        encodedStream.writeInt(valueCount);
        writeObject(lastValue);
        break;
      case NONREPEATED_RUN:
        encodedStream.writeInt(valueCount | 0x80000000);
        for (int i = 0; i < valueCount; i++) {
          writeObject(nonRepeatValues.get(i));
        }
        break;
      default:
        assert (runState == RUN_STATE.INITIALIZING);
        encodedStream.writeInt(1);
        writeObject(lastValue);
    }
  }

  // after each run, call this to initialize the state and collected data
  private void initialize(Object value) {
    runState = RUN_STATE.INITIALIZING;
    valueCount = 1;
    lastValue = value;
    nonRepeatValues.clear();
  }

  // repeated run ends, put the collected data to result page
  private void encodeRepeatedRun() throws IOException {
    // put the value count (highest bit is 1) and all collected values
    encodedStream.writeInt(valueCount | 0x80000000);
    for (int i = 0; i < valueCount; i++) {
      writeObject(nonRepeatValues.get(i));
    }
  }

  // non-repeated run ends, put repeated value to result page
  private void encodeNonRepeatedRun() throws IOException {
    // put the value count (highest bit is 0) and repeated value
    encodedStream.writeInt(valueCount);
    writeObject(lastValue);
  }

  private void putRepeatValue(Object value) throws IOException {
    switch (runState) {
      case REPEATED_RUN:
        valueCount++;
        break;
      case NONREPEATED_RUN:
        // non-repeated run ends, encode this run
        encodeRepeatedRun();
        initialize(value);
        break;
      default:
        assert (runState == RUN_STATE.INITIALIZING);
        // enter repeated run
        runState = RUN_STATE.REPEATED_RUN;
        valueCount++;
        break;
    }
  }

  private void putNonRepeatValue(Object value) throws IOException {
    switch (runState) {
      case NONREPEATED_RUN:
        // collect the non-repeated value
        nonRepeatValues.add(value);
        valueCount++;
        break;
      case REPEATED_RUN:
        // repeated-run ends, encode this run
        encodeNonRepeatedRun();
        initialize(value);
        break;
      default:
        assert (runState == RUN_STATE.INITIALIZING);
        // enter non-repeated run
        runState = RUN_STATE.NONREPEATED_RUN;
        nonRepeatValues.add(lastValue);
        nonRepeatValues.add(value);
        valueCount++;
        break;
    }
  }

  // RLE encode implementation
  private void encodeObject(int rowId, Object value) throws IOException {
    if (rowId == 0) {
      initialize(value);
    } else {
      if (lastValue.equals(value)) {
        putRepeatValue(value);
      } else {
        putNonRepeatValue(value);
      }
    }
    if (rowId == inputPageSize - 1) {
      onEncodingLastRow(rowId);
    }
  }

  @Override
  public void encode(int rowId, byte value) throws IOException {
    encodeObject(rowId, value);
  }

  @Override
  public void encode(int rowId, short value) throws IOException {
    encodeObject(rowId, value);
  }

  @Override
  public void encode(int rowId, int value) throws IOException {
    encodeObject(rowId, value);
  }

  @Override
  public void encode(int rowId, long value) throws IOException {
    encodeObject(rowId, value);
  }

  @Override
  public void encode(int rowId, float value) throws IOException {
    encodeObject(rowId, value);
  }

  @Override
  public void encode(int rowId, double value) throws IOException {
    encodeObject(rowId, value);
  }

  byte[] getEncodedBytes() {
    return bao.toByteArray();
  }

  @Override
  public long decodeLong(byte value) {
    return 0;
  }

  @Override
  public long decodeLong(short value) {
    return 0;
  }

  @Override
  public long decodeLong(int value) {
    return 0;
  }

  @Override
  public double decodeDouble(byte value) {
    return 0;
  }

  @Override
  public double decodeDouble(short value) {
    return 0;
  }

  @Override
  public double decodeDouble(int value) {
    return 0;
  }

  @Override
  public double decodeDouble(long value) {
    return 0;
  }

  @Override
  public double decodeDouble(float value) {
    return 0;
  }

  @Override
  public double decodeDouble(double value) {
    return 0;
  }
}