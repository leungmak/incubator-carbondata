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

package org.apache.carbondata.core.util;

import org.apache.carbondata.core.api.CarbonProperties;
import org.apache.carbondata.core.stats.DriverQueryStatisticsRecorderDummy;
import org.apache.carbondata.core.stats.DriverQueryStatisticsRecorderImpl;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;
import org.apache.carbondata.core.stats.QueryStatisticsRecorderDummy;
import org.apache.carbondata.core.stats.QueryStatisticsRecorderImpl;

public class CarbonTimeStatisticsFactory {
  private static boolean loadStatisticsEnabled;
  private static LoadStatistics loadStatisticsInstance;
  private static boolean driverRecorderEnabled;
  private static QueryStatisticsRecorder driverRecorder;

  static {
    CarbonTimeStatisticsFactory.updateTimeStatisticsUtilStatus();
    loadStatisticsInstance = genLoadStatisticsInstance();
    driverRecorder = genDriverRecorder();
  }

  private static void updateTimeStatisticsUtilStatus() {
    loadStatisticsEnabled = CarbonProperties.ENABLE_DATA_LOADING_STATISTICS.getOrDefault();
    driverRecorderEnabled = CarbonProperties.ENABLE_QUERY_STATISTICS.getOrDefault();
  }

  private static LoadStatistics genLoadStatisticsInstance() {
    if (loadStatisticsEnabled) {
      return CarbonLoadStatisticsImpl.getInstance();
    } else {
      return CarbonLoadStatisticsDummy.getInstance();
    }
  }

  public static LoadStatistics getLoadStatisticsInstance() {
    return loadStatisticsInstance;
  }

  private static QueryStatisticsRecorder genDriverRecorder() {
    if (driverRecorderEnabled) {
      return DriverQueryStatisticsRecorderImpl.getInstance();
    } else {
      return DriverQueryStatisticsRecorderDummy.getInstance();
    }
  }

  public static QueryStatisticsRecorder createDriverRecorder() {
    return driverRecorder;
  }

  public static QueryStatisticsRecorder createExecutorRecorder(String queryId) {
    if (CarbonProperties.ENABLE_QUERY_STATISTICS.getOrDefault()) {
      return new QueryStatisticsRecorderImpl(queryId);
    } else {
      return new QueryStatisticsRecorderDummy();
    }
  }

}