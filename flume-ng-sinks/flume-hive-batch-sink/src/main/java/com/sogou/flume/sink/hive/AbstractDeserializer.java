package com.sogou.flume.sink.hive;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Tao Li on 2/16/16.
 */
public abstract class AbstractDeserializer implements Deserializer {
  private ObjectInspector rowOI;
  private List<Object> row = new ArrayList<Object>();

  @Override
  public void initialize(String columnNameProperty, String columnTypeProperty) {
    List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
    List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    StructTypeInfo rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    rowOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return rowOI;
  }

  @Override
  public Object deserialize(byte[] bytes) {
    row.clear();
    return deserialize(bytes, row);
  }

  public abstract List<Object> deserialize(byte[] bytes, List<Object> reuse);
}
