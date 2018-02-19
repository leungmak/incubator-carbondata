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

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.command.preaaggregate.CarbonCreatePreAggregateTableCommand;

public class PreAggregateDataMapProvider implements DataMapProvider {
  protected CarbonCreatePreAggregateTableCommand command;

  @Override
  public void create(CarbonTable mainTable, DataMapSchema dataMapSchema, String ctasSqlStatement,
      SparkSession sparkSession) {
    command = new CarbonCreatePreAggregateTableCommand(mainTable, dataMapSchema.getDataMapName(),
        dataMapSchema.getProviderName(), dataMapSchema.getProperties(), ctasSqlStatement, null);
    command.processMetadata(sparkSession);
  }

  @Override
  public void shutdown(CarbonTable mainTable, DataMapSchema dataMapSchema,
      SparkSession sparkSession) {
    command.undoMetadata(sparkSession, null);
  }

  @Override
  public void rebuild(CarbonTable mainTable, SparkSession sparkSession) {
    if (command != null) {
      command.processData(sparkSession);
    }
  }

  @Override
  public void incrementalBuild(CarbonTable mainTable, String[] segmentIds,
      SparkSession sparkSession) {
    throw new UnsupportedOperationException();
  }
}
