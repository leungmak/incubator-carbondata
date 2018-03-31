package org.apache.carbondata.store;

public interface Row {
  boolean getBoolean(int index);
  short getShort(int index);
  int getInt(int index);
  long getLong(int index);
  double getDouble(int index);
  String getString(int index);
}
