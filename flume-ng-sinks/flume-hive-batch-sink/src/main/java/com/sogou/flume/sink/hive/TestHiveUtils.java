package com.sogou.flume.sink.hive;

import com.sogou.flume.sink.hive.util.HiveUtils;
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
    String logdate = args[0];
    List<String> values = new ArrayList<String>();
    values.add(logdate);
    String location = "hdfs://SunshineNameNode2/user/hive/warehouse/litao.db/testorc/logdate=201602021250/";

    HiveUtils.addPartition(dbName, tableName, values, location);
  }
}
