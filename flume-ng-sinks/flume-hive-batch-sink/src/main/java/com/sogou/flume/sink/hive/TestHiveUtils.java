package com.sogou.flume.sink.hive;

import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tao Li on 2/16/16.
 */
public class TestHiveUtils {
  public static void main(String[] args) throws TException {
    String dbName = "custom";
    String tableName = "helloorc2";
    String columnNameProperty = "f0,f1,f2,f3,f4";
    String columnTypeProperty = "string,int,array<string>,map<string,string>,array<struct<pos:int,url:string>>";
    String logdate = args[0];
    List<String> values = new ArrayList<String>();
    values.add(logdate);
    String location = "hdfs://SunshineNameNode2/user/hive/warehouse/litao.db/testorc/logdate=201602021250/";

    HiveUtils.addPartition(dbName, tableName, columnNameProperty, columnTypeProperty, values, location);
  }
}
