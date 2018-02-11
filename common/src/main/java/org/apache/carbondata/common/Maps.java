package org.apache.carbondata.common;

import java.util.Map;

public class Maps {

  /**
   * Return value if key is contained in the map, else return defauleValue.
   * This is added to avoid JDK 8 dependency
   */
  public static <K, V> V getOrDefault(Map<K, V> map, K key, V defaultValue) {
    V value = map.get(key);
    if (value != null) {
      return value;
    } else {
      return defaultValue;
    }
  }
}
