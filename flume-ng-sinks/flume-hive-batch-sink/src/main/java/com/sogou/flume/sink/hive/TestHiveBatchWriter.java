package com.sogou.flume.sink.hive;

import com.sogou.flume.sink.hive.deserializer.AbstractDeserializer;
import com.sogou.flume.sink.hive.deserializer.Deserializer;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Tao Li on 2/16/16.
 */
public class TestHiveBatchWriter {
  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    Deserializer deserializer = new MyDeserializer();
    deserializer.initialize("f0,f1,f2,f3,f4", "string,int,array<string>,map<string,string>,array<struct<pos:int,url:string>>");
    String logdate = args[0];
    long currentTimestamp = System.currentTimeMillis();
    String file = String.format("hdfs://SunshineNameNode2/user/hive/warehouse/litao.db/testorc/logdate=%s/helloorc-%s-%s.orc",
        logdate, logdate, currentTimestamp);
    List<HiveBatchWriter.Callback> initCallBacks = new ArrayList<HiveBatchWriter.Callback>();
    initCallBacks.add(new InitCallback(logdate));
    List<HiveBatchWriter.Callback> closeCallBacks = new ArrayList<HiveBatchWriter.Callback>();
    closeCallBacks.add(new CloseCallback(logdate));

    HiveBatchWriter writer = new HiveBatchWriter(conf, deserializer, file, 5000,
        initCallBacks, closeCallBacks);

    for (int i = 0; i < 9; i++) {
      String testLine = String.format("hello%d %d e1%d,e2%d k1=v1%d&k2=v2%d 1%d:url1%d,2%d:url2%d", i, i, i, i, i, i, i, i, i, i, i, i);
      byte[] bytes = testLine.getBytes();
      writer.append(bytes);
    }

    writer.close();
  }
}

class MyDeserializer extends AbstractDeserializer {

  @Override
  public List<Object> deserialize(byte[] bytes, List<Object> reuse) {
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

class InitCallback implements HiveBatchWriter.Callback {
  private String logdate;

  public InitCallback(String logdate) {
    this.logdate = logdate;
  }

  @Override
  public void run() {
    System.out.println("InitCallback: " + logdate);
  }
}

class CloseCallback implements HiveBatchWriter.Callback {
  private String logdate;

  public CloseCallback(String logdate) {
    this.logdate = logdate;
  }

  @Override
  public void run() {
    System.out.println("CloseCallback: " + logdate);
  }
}