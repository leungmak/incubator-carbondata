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

package org.apache.carbondata.core.scan.filter.executer;

import java.io.IOException;
import java.util.BitSet;

import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.util.path.CarbonTablePath;

/**
 * This class will implement the blocklet and block pruning logic based
 * on the implicit column filter values
 */
public class ImplicitIncludeFilterExecutorImpl
    implements FilterExecuter, ImplicitColumnFilterExecutor {

  private final DimColumnResolvedFilterInfo dimColumnEvaluatorInfo;

  public ImplicitIncludeFilterExecutorImpl(DimColumnResolvedFilterInfo dimColumnEvaluatorInfo) {
    this.dimColumnEvaluatorInfo = dimColumnEvaluatorInfo;
  }

  @Override
  public BitSetGroup applyFilter(BlocksChunkHolder blockChunkHolder, boolean useBitsetPipeline)
      throws FilterUnsupportedException {
    BitSetGroup bitSetGroup = new BitSetGroup(blockChunkHolder.getDataBlock().numberOfPages());
    for (int i = 0; i < blockChunkHolder.getDataBlock().numberOfPages(); i++) {
      bitSetGroup.setBitSet(
          setBitSetForCompleteDimensionData(blockChunkHolder.getDataBlock().getPageRowCount(i)), i);
    }
    return bitSetGroup;
  }

  @Override public boolean applyFilter(RowIntf value, int dimOrdinalMax)
      throws FilterUnsupportedException, IOException {
    return false;
  }

  @Override public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
    return null;
  }

  @Override public void readBlocks(BlocksChunkHolder blockChunkHolder) throws IOException {

  }

  @Override
  public BitSet isFilterValuesPresentInBlockOrBlocklet(byte[][] maxValue, byte[][] minValue,
      String uniqueBlockPath) {
    BitSet bitSet = new BitSet(1);
    boolean isScanRequired = false;
    String shortBlockId = CarbonTablePath.getShortBlockId(uniqueBlockPath);
    if (uniqueBlockPath.endsWith(".carbondata")) {
      if (dimColumnEvaluatorInfo.getFilterValues().getImplicitDriverColumnFilterList()
          .contains(shortBlockId)) {
        isScanRequired = true;
      }
    } else if (dimColumnEvaluatorInfo.getFilterValues().getImplicitColumnFilterList()
        .contains(shortBlockId)) {
      isScanRequired = true;
    }
    if (isScanRequired) {
      bitSet.set(0);
    }
    return bitSet;
  }

  /**
   * For implicit column filtering, complete data need to be selected. As it is a special case
   * no data need to be discarded, implicit filtering is only for slecting block and blocklets
   *
   * @param numberOfRows
   * @return
   */
  private BitSet setBitSetForCompleteDimensionData(int numberOfRows) {
    BitSet bitSet = new BitSet();
    bitSet.set(0, numberOfRows, true);
    return bitSet;
  }

  @Override
  public Boolean isFilterValuesPresentInAbstractIndex(byte[][] maxValue, byte[][] minValue) {
    return true;
  }
}
