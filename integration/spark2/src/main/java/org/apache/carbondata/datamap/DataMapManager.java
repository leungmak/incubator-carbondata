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

package org.apache.carbondata.datamap;

import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.processing.exception.DataLoadingException;

import org.apache.spark.sql.SparkSession;

public class DataMapManager {

  private static DataMapManager INSTANCE;

  private DataMapManager() { }

  public static DataMapManager get() {
    if (INSTANCE == null) {
      INSTANCE = new DataMapManager();
    }
    return INSTANCE;
  }

  /**
   * Create a new DataMap for specified mainTable. The metadata of mainTable will
   * be updated and datamap will be loaded based on mainTable's 'datamapAutoRefresh'
   * table property is set to true.
   *
   * @param mainTable main table object
   * @param dataMapSchema schema of the datamap
   * @param sparkSession spark session
   */
  public DataMapProvider createDataMap(CarbonTable mainTable, DataMapSchema dataMapSchema,
      String ctasSqlStatement, SparkSession sparkSession)
      throws MalformedDataMapCommandException {
    DataMapProvider provider;
    if (dataMapSchema.getProviderName().equalsIgnoreCase(
        org.apache.carbondata.core.metadata.schema.datamap.DataMapProvider.PREAGGREGATE.toString()))
    {
      provider = new PreAggregateDataMapProvider();
    } else if (dataMapSchema.getProviderName().equalsIgnoreCase(
        org.apache.carbondata.core.metadata.schema.datamap.DataMapProvider.TIMESERIES.toString())) {
      provider = new TimeseriesDataMapProvider();
    } else {
      provider = new IndexDataMapProvider();
    }
    provider.init(mainTable, dataMapSchema, ctasSqlStatement, sparkSession);
    return provider;
  }

  void dropDataMap() {

  }

  public void rebuildDataMap(CarbonTable table, DataMapProvider provider,
      SparkSession sparkSession) throws DataLoadingException {
    provider.rebuild(sparkSession);
  }

}
