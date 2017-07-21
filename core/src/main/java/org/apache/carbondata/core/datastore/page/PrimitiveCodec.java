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

package org.apache.carbondata.core.datastore.page;

import java.io.IOException;

// Transformation type that can be applied to ColumnPage
public interface PrimitiveCodec {
  void encode(int rowId, byte value) throws IOException;
  void encode(int rowId, short value) throws IOException;
  void encode(int rowId, int value) throws IOException;
  void encode(int rowId, long value) throws IOException;
  void encode(int rowId, float value) throws IOException;
  void encode(int rowId, double value) throws IOException;

  long decodeLong(byte value);
  long decodeLong(short value);
  long decodeLong(int value);
  double decodeDouble(byte value);
  double decodeDouble(short value);
  double decodeDouble(int value);
  double decodeDouble(long value);
  double decodeDouble(float value);
  double decodeDouble(double value);
}
