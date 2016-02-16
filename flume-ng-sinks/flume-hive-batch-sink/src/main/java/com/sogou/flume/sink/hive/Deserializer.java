package com.sogou.flume.sink.hive;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * Created by Tao Li on 2/16/16.
 */
public interface Deserializer {
  ObjectInspector getObjectInspector();

  Object deserialize(byte[] bytes);
}
