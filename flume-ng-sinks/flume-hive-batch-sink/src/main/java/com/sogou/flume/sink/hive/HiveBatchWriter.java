package com.sogou.flume.sink.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by Tao Li on 2/16/16.
 */
public class HiveBatchWriter {
  private static final Logger LOG = LoggerFactory.getLogger(HiveBatchWriter2.class);

  private long lastWriteTime = -1;  // -1: no data write yet
  private Writer writer;
  private Deserializer deserializer;
  private long idleTimeout = 5000;

  public interface Callback {
    void run();
  }

  public HiveBatchWriter(Configuration conf, Deserializer deserializer, String file,
                         long idleTimeout, List<Callback> callbacks) throws IOException {
    this.deserializer = deserializer;
    this.idleTimeout = idleTimeout;
    OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf);
    writerOptions.inspector(deserializer.getObjectInspector());
    this.writer = OrcFile.createWriter(new Path(file), writerOptions);
    for (Callback callback : callbacks) {
      callback.run();
    }
  }

  public void append(byte[] bytes) throws IOException {
    writer.addRow(deserializer.deserialize(bytes));
    lastWriteTime = System.currentTimeMillis();
  }

  public void close(List<Callback> callbacks) throws IOException {
    writer.close();
    for (Callback callback : callbacks) {
      callback.run();
    }
  }

  public boolean isIdle() {
    return lastWriteTime > 0 && System.currentTimeMillis() - lastWriteTime >= idleTimeout;
  }
}
