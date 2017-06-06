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

// Transformation type that can be applied to ColumnPage
public enum ColumnPageTransform {
  // no operation, new value = page value
  NO_OP,

  // new value = (max value of page) - (page value)
  MAX_DELTA,

  // new value = (10 power of decimal) * (page value)
  UPSCALE,

  // new value = (10 power of decimal) * ((max value of page) - (page value))
  UPSCALE_MAX_DELTA
}
