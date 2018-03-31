package org.apache.carbondata.store;

import java.io.IOException;

import org.apache.carbondata.core.scan.expression.Expression;

public interface CarbonStore {

  Row[] query(
      String path,
      String[] projectColumns,
      Expression filter) throws IOException;

  Row[] sql(String sqlString) throws IOException;

}
