package com.sogou.flume.sink.hive.batch;

import org.apache.flume.sink.hive.batch.HiveBatchWriter;
import org.apache.flume.sink.hive.batch.callback.AddPartitionCallback;
import org.apache.flume.sink.hive.batch.serde.TextDeserializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    HiveBatchWriter writer = new HiveBatchWriter(conf, deserializer, file, 5000, null, closeCallBacks);
    for (int i = 0; i < 9; i++) {
      String testLine = String.format("hello%d %d e1%d,e2%d k1=v1%d&k2=v2%d 1%d:url1%d,2%d:url2%d", i, i, i, i, i, i, i, i, i, i, i, i);
      byte[] bytes = testLine.getBytes();
      writer.append(bytes);
    }
    writer.close();
  }
}

class MyDeserializer extends TextDeserializer {

  @Override
  public Object deserialize(byte[] bytes, List<Object> reuse) {
    String line = new String(bytes);
    String[] array = line.split(" ");

    // f0: string
    String f0 = array[0];

    // f1: int
    int f1 = Integer.parseInt(array[1]);

    // f2: array<string>
    List<String> f2 = new ArrayList<String>();
    for (String e : array[2].split(",")) {
      f2.add(e);
    }

    // f3: map<string,string>
    Map<String, String> f3 = new HashMap<String, String>();
    for (String kvStr : array[3].split("&")) {
      String[] kv = kvStr.split("=");
      f3.put(kv[0], kv[1]);
    }

    // f4: array<struct<pos:int,url:string>>
    List<Object> f4 = new ArrayList<Object>();
    for (String str : array[4].split(",")) {
      String[] kv = str.split(":");
      f4.add(new Object[]{Integer.parseInt(kv[0]), kv[1]});
    }

    reuse.add(0, f0);
    reuse.add(1, f1);
    reuse.add(2, f2);
    reuse.add(3, f3);
    reuse.add(4, f4);

    return reuse;
  }
}