package org.apache.carbondata.processing.newflow.mdkstep;

import java.util.Iterator;

import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.DataLoadProcessorStep;

public class MDKGeneratorProcessorStepImpl implements DataLoadProcessorStep {

  @Override public DataField[] getOutput() {
    return new DataField[0];
  }

  @Override
  public void intialize(CarbonDataLoadConfiguration configuration, DataLoadProcessorStep child) {

  }

  @Override public Iterator<Object[]> execute() {
    return null;
  }

  @Override public void close() {

  }
}
