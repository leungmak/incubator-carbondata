/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.carbon.datastore.block.BlockletInfos;
import org.apache.carbondata.core.carbon.datastore.block.Distributable;
import org.apache.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Carbon input split to allow distributed read of CarbonInputFormat.
 */
public class CarbonInputSplit extends FileSplit implements Distributable, Serializable, Writable {

  private static final long serialVersionUID = 3520344046772190207L;
  private String segmentId;
  public String taskId = "0";

  /*
   * Number of BlockLets in a block
   */
  private int numberOfBlocklets = 0;

  public  CarbonInputSplit() {
  }

  private void parserPath(Path path){
	 String[] nameParts = path.getName().split("-");
	 if(nameParts != null && nameParts.length >= 3){
		 this.taskId = nameParts[2];
	 }
  }
  
  private CarbonInputSplit(String segmentId, Path path, long start, long length,
      String[] locations) {
    super(path, start, length, locations);
    this.segmentId = segmentId;
  }

  public CarbonInputSplit(String segmentId, Path path, long start, long length,
      String[] locations, int numberOfBlocklets) {
    this(segmentId, path, start, length, locations);
    this.numberOfBlocklets = numberOfBlocklets;
  }

  public static CarbonInputSplit from(String segmentId, FileSplit split) throws IOException {
    return new CarbonInputSplit(segmentId, split.getPath(), split.getStart(), split.getLength(),
        split.getLocations());
  }

  public String getSegmentId() {
    return segmentId;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.segmentId = in.readUTF();

  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(segmentId);
  }

  /**
   * returns the number of blocklets
   * @return
   */
  public int getNumberOfBlocklets() {
    return numberOfBlocklets;
  }

  @Override
  public int compareTo(Distributable o) {
    CarbonInputSplit other = (CarbonInputSplit)o;
    int compareResult = 0;
    // get the segment id
    // converr seg ID to double.

    double seg1 = Double.parseDouble(segmentId);
    double seg2 = Double.parseDouble(other.getSegmentId());
    if (seg1 - seg2 < 0) {
      return -1;
    }
    if (seg1 - seg2 > 0) {
      return 1;
    }

    // Comparing the time task id of the file to other
    // if both the task id of the file is same then we need to compare the
    // offset of
    // the file
    String filePath1 = this.getPath().getName();
    String filePath2 = other.getPath().getName();
    if (CarbonTablePath.isCarbonDataFile(filePath1)) {
      int firstTaskId = Integer.parseInt(CarbonTablePath.DataFileUtil.getTaskNo(filePath1));
      int otherTaskId = Integer.parseInt(CarbonTablePath.DataFileUtil.getTaskNo(filePath2));
      if (firstTaskId != otherTaskId) {
        return firstTaskId - otherTaskId;
      }
      // compare the part no of both block info
      int firstPartNo = Integer.parseInt(CarbonTablePath.DataFileUtil.getPartNo(filePath1));
      int SecondPartNo = Integer.parseInt(CarbonTablePath.DataFileUtil.getPartNo(filePath2));
      compareResult = firstPartNo - SecondPartNo;
    } else {
      compareResult = filePath1.compareTo(filePath2);
    }
    if (compareResult != 0) {
      return compareResult;
    }
//    //compare result is not 0 then return
//    // if part no is also same then compare the offset and length of the block
//    if (getLength() + blockLength
//        < ((TableBlockInfo) other).blockOffset + ((TableBlockInfo) other).blockLength) {
//      return -1;
//    } else if (blockOffset + blockLength
//        > ((TableBlockInfo) other).blockOffset + ((TableBlockInfo) other).blockLength) {
//      return 1;
//    }
//    //compare the startBlockLetNumber
//    int diffStartBlockLetNumber =
//        blockletInfos.getStartBlockletNumber() - ((TableBlockInfo) other).blockletInfos
//            .getStartBlockletNumber();
//    if (diffStartBlockLetNumber < 0) {
//      return -1;
//    }
//    if (diffStartBlockLetNumber > 0) {
//      return 1;
//    }
    return 0;
  }

  public static List<TableBlockInfo> createBlocks(List<CarbonInputSplit> splitList) {
    List<TableBlockInfo> tableBlockInfoList = new ArrayList<>();
    for (CarbonInputSplit split: splitList) {
      BlockletInfos blockletInfos = new BlockletInfos(split.getNumberOfBlocklets(), 0,
          split.getNumberOfBlocklets());
      try {
        tableBlockInfoList.add(
            new TableBlockInfo(split.getPath().toString(), split.getStart(), split.getSegmentId(),
                split.getLocations(), split.getLength(), blockletInfos));
      } catch (IOException e) {
        throw new RuntimeException("fail to get location of split: " + split, e);
      }
    }
    return tableBlockInfoList;
  }
}
