package com.sogou.flume.sink.hive.batch.test;

import org.apache.flume.sink.hive.batch.HiveBatchWriter;
import org.apache.flume.sink.hive.batch.callback.AddPartitionCallback;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tao Li on 2/16/16.
 */
public class TestHiveBatchWriter {
  public static void main(String[] args) throws IOException, SerDeException, TException {
    String dbName = "custom";
    String tableName = "helloorc2";
    String logdate = args[0];

    Configuration conf = new Configuration();

    MyDeserializer deserializer = new MyDeserializer();
    deserializer.initializeByTableName(conf, dbName, tableName);

    long currentTimestamp = System.currentTimeMillis();
    String location = String.format("hdfs://SunshineNameNode2/user/hive/warehouse/litao.db/testorc/logdate=%s", logdate);
    String file = String.format("%s/helloorc-%s-%s.orc", location, logdate, currentTimestamp);

    List<String> values = new ArrayList<String>();
    values.add(logdate);
    List<HiveBatchWriter.Callback> closeCallBacks = new ArrayList<HiveBatchWriter.Callback>();
    closeCallBacks.add(new AddPartitionCallback(dbName, tableName, values, location));

    HiveBatchWriter writer = new HiveBatchWriter(conf, deserializer, file);
    writer.setCloseCallbacks(closeCallBacks);
    for (int i = 0; i < 9; i++) {
      String testLine = String.format("hello%d %d e1%d,e2%d k1=v1%d&k2=v2%d 1%d:url1%d,2%d:url2%d", i, i, i, i, i, i, i, i, i, i, i, i);
      byte[] bytes = testLine.getBytes();
      writer.append(bytes);
    }
    writer.close();
  }
}