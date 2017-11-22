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

package org.apache.carbondata.core.metadata.datatype;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.core.metadata.schema.table.MalformedCarbonCommandException;

public class StructType extends DataType {

  private List<StructField> fields;

  StructType(List<StructField> fields) {
    super(DataTypes.STRUCT_TYPE_ID, 10, "STRUCT", -1);
    this.fields = fields;
  }

  @Override
  public boolean isComplexType() {
    return true;
  }

  public List<StructField> getFields() {
    return fields;
  }

  public int getNumOfChild() {
    int numOfChild = 0;
    for (StructField field : fields) {
      numOfChild += field.getDataType().getNumOfChild();
    }
    return numOfChild;
  }

  public void validateSchema() throws MalformedCarbonCommandException {
    Set<String> fieldNames = new HashSet<>();
    for (StructField field : fields) {
      if (fieldNames.contains(field.getFieldName())) {
        throw new MalformedCarbonCommandException(
            "Duplicate column found with name " + field.getFieldName());
      }
      fieldNames.add(field.getFieldName());
    }
  }
}
