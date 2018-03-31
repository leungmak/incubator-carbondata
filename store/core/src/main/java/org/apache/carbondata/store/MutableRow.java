package org.apache.carbondata.store;

class MutableRow implements Row {
  @Override public boolean getBoolean(int index) {
    return false;
  }

  @Override public short getShort(int index) {
    return 0;
  }

  @Override public int getInt(int index) {
    return 0;
  }

  @Override public long getLong(int index) {
    return 0;
  }

  @Override public double getDouble(int index) {
    return 0;
  }

  @Override public String getString(int index) {
    return null;
  }
}
