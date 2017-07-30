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

package org.apache.carbondata.core.datastore.page.encoding.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.metadata.datatype.DataType;

// RLE encode implementation
public class RLEEncoderStream implements EncoderStream {


  enum RUN_STATE { INIT, START, REPEATED_RUN, NONREPEATED_RUN}

  // While encoding RLE, this class internally work as a state machine
  // INIT state is the initial state before any value comes
  // START state is the start for each run
  // REPEATED_RUN state means it is collecting repeated values (`lastValue`)
  // NONREPEATED_RUN state means it is collecting non-repeated values (`nonRepeatValues`)
  private RUN_STATE runState;

  // count for each run, either REPEATED_RUN or NONREPEATED_RUN
  private int valueCount;

  // collected value for REPEATED_RUN
  private Object lastValue;

  // collected value for NONREPEATED_RUN
  private List<Object> nonRepeatValues;

  private DataType dataType;

  private EncoderStream child;

  private boolean firstValue;

  public RLEEncoderStream(EncoderStream child) {
    this.child = child;
  }

  @Override
  public void start() {
    this.runState = RUN_STATE.INIT;
    this.valueCount = 0;
    this.nonRepeatValues = new ArrayList<>();
  }

  @Override
  public void write(byte value) throws IOException {
    writeObject(value);
  }

  @Override
  public void write(short value) throws IOException {
    writeObject(value);
  }

  @Override
  public void write(int value) throws IOException {
    writeObject(value);
  }

  @Override
  public void write(long value) throws IOException {
    writeObject(value);
  }

  private void writeObject(Object value) throws IOException {
    if (runState == RUN_STATE.INIT) {
      startRun(value);
    } else {
      if (lastValue.equals(value)) {
        putRepeatValue(value);
      } else {
        putNonRepeatValue(value);
      }
    }
  }

  // when last row is reached, write out all collected data
  @Override
  public byte[] end() throws IOException {
    switch (runState) {
      case REPEATED_RUN:
        child.write(valueCount);
        writeValue(lastValue);
        break;
      case NONREPEATED_RUN:
        child.write(valueCount | 0x80000000);
        for (int i = 0; i < valueCount; i++) {
          writeValue(nonRepeatValues.get(i));
        }
        break;
      default:
        assert (runState == RUN_STATE.START);
        child.write(1);
        writeValue(lastValue);
    }
    return child.end();
  }

  private void writeValue(Object value) throws IOException {
    switch (dataType) {
      case BYTE:
        child.write((byte) value);
        break;
      case SHORT:
        child.write((short) value);
        break;
      case INT:
        child.write((int) value);
        break;
      case LONG:
        child.write((long) value);
        break;
      default:
        throw new RuntimeException("internal error");
    }
  }

  // after each run, call this to initialize the state and collected data
  private void startRun(Object value) {
    runState = RUN_STATE.START;
    valueCount = 1;
    lastValue = value;
    nonRepeatValues.clear();
    nonRepeatValues.add(value);
  }

  // repeated run ends, put the collected data to result page
  private void encodeRepeatedRun() throws IOException {
    // put the value count (highest bit is 1) and all collected values
    child.write(valueCount | 0x80000000);
    for (int i = 0; i < valueCount; i++) {
      writeValue(nonRepeatValues.get(i));
    }
  }

  // non-repeated run ends, put repeated value to result page
  private void encodeNonRepeatedRun() throws IOException {
    // put the value count (highest bit is 0) and repeated value
    child.write(valueCount);
    writeValue(lastValue);
  }

  private void putRepeatValue(Object value) throws IOException {
    switch (runState) {
      case REPEATED_RUN:
        valueCount++;
        break;
      case NONREPEATED_RUN:
        // non-repeated run ends, encode this run
        encodeRepeatedRun();
        startRun(value);
        break;
      default:
        assert (runState == RUN_STATE.START);
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
        startRun(value);
        break;
      default:
        assert (runState == RUN_STATE.START);
        // enter non-repeated run
        runState = RUN_STATE.NONREPEATED_RUN;
        nonRepeatValues.add(value);
        valueCount++;
        break;
    }
  }

}
