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
package org.apache.carbondata.core.metadata.encoder;

/**
 * Encoding type supported in carbon
 */
public enum Encoding {
  DICTIONARY,
  DELTA,
  RLE,
  INVERTED_INDEX,
  BIT_PACKED,
  DIRECT_DICTIONARY,
  IMPLICIT,

  DIRECT_COMPRESS,
  ADAPTIVE;

  public static Encoding valueOf(int ordinal) {
    if (ordinal == DICTIONARY.ordinal()) {
      return DICTIONARY;
    } else if (ordinal == DELTA.ordinal()) {
      return DELTA;
    } else if (ordinal == RLE.ordinal()) {
      return RLE;
    } else if (ordinal == INVERTED_INDEX.ordinal()) {
      return INVERTED_INDEX;
    } else if (ordinal == BIT_PACKED.ordinal()) {
      return BIT_PACKED;
    } else if (ordinal == DIRECT_DICTIONARY.ordinal()) {
      return DIRECT_DICTIONARY;
    } else if (ordinal == IMPLICIT.ordinal()) {
      return IMPLICIT;
    } else {
      throw new RuntimeException("create Encoding with invalid ordinal: " + ordinal);
    }
  }
}
