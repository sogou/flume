package com.sogou.flume.sink.hive;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Created by Tao Li on 2016/2/17.
 */
public class HiveBatchSink extends AbstractSink implements Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(HiveBatchWriter.class);

  private static String DIRECTORY_DELIMITER = System.getProperty("file.separator");

  private String database;
  private String table;
  private String filePath;
  private String fileName;
  private TimeZone timeZone;
  private int maxOpenFiles;
  private long batchSize;
  private boolean needRounding = false;
  private int roundUnit = Calendar.SECOND;
  private int roundValue = 1;
  private boolean useLocalTime = false;
  private Configuration conf = new Configuration();
  private Deserializer deserializer;
  private long idleTimeout = 5000;

  private final Object writersLock = new Object();
  private WriterLinkedHashMap writers;
  private IdleHiveBatchWriterMonitor idleWriterMonitor;
  private boolean isRunning = false;

  private class WriterLinkedHashMap extends LinkedHashMap<String, HiveBatchWriter> {
    private final int maxOpenFiles;

    public WriterLinkedHashMap(int maxOpenFiles) {
      super(16, 0.75f, true);
      this.maxOpenFiles = maxOpenFiles;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, HiveBatchWriter> eldest) {
      if (this.size() > maxOpenFiles) {
        try {
          // when exceed maxOpenFiles, close the eldest writer
          eldest.getValue().close();
        } catch (IOException e) {
          LOG.warn(eldest.getKey().toString(), e);
        }
        return true;
      } else {
        return false;
      }
    }
  }

  private class IdleHiveBatchWriterMonitor implements Runnable {
    private final long CHECK_INTERVAL = 5;

    @Override
    public void run() {
      while (isRunning) {
        try {
          for (Map.Entry<String, HiveBatchWriter> entry : writers.entrySet()) {
            String path = entry.getKey();
            HiveBatchWriter writer = entry.getValue();
            synchronized (writersLock) {
              if (writer.isIdle()) {
                try {
                  LOG.info("Closing {}", path);
                  writer.close();
                  writers.remove(path);
                } catch (IOException e) {
                  LOG.warn("Exception while closing " + path + ". " + "Exception follows.", e);
                }
              }
            }
          }
          TimeUnit.SECONDS.sleep(CHECK_INTERVAL);
        } catch (Exception e) {
          LOG.error("exception occur", e);
        }
      }
    }
  }

  private class DaemonThreadFactory implements ThreadFactory {
    private String name;

    public DaemonThreadFactory(String name) {
      this.name = name;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(r);
      thread.setName(name);
      thread.setDaemon(true);
      return thread;
    }
  }

  @Override

  public void configure(Context context) {
    database = context.getString(Config.HIVE_DATABASE, Config.Default.DEFAULT_DATABASE);
    table = Preconditions.checkNotNull(context.getString(Config.HIVE_TABLE),
        Config.HIVE_TABLE + " is required");
    filePath = Preconditions.checkNotNull(context.getString(Config.HIVE_PATH),
        Config.HIVE_PATH + " is required");
    fileName = context.getString(Config.HIVE_FILE_NAME, Config.Default.DEFAULT_FILE_NAME);
    String tzName = context.getString(Config.HIVE_TIME_ZONE);
    timeZone = tzName == null ? null : TimeZone.getTimeZone(tzName);
    maxOpenFiles = context.getInteger(Config.HIVE_MAX_OPEN_FILES, Config.Default.DEFAULT_MAX_OPEN_FILES);
    batchSize = context.getLong(Config.HIVE_BATCH_SIZE, Config.Default.DEFAULT_BATCH_SIZE);
    String deserializerName = Preconditions.checkNotNull(context.getString(Config.HIVE_DESERIALIZER),
        Config.HIVE_DESERIALIZER + " is required");
    try {
      this.deserializer = (Deserializer) Class.forName(deserializerName).newInstance();
      List<FieldSchema> fields = HiveUtils.getFields(database, table);
      List<String> columnNames = new ArrayList<String>();
      List<String> columnTypes = new ArrayList<String>();
      for (FieldSchema field : fields) {
        columnNames.add(field.getName());
        columnTypes.add(field.getType());
      }
      String columnNameProperty = Joiner.on(",").join(columnNames);
      String columnTypeProperty = Joiner.on(",").join(columnTypes);
      this.deserializer.initialize(columnNameProperty, columnTypeProperty);
    } catch (Exception e) {
      throw new IllegalArgumentException(deserializerName + " init failed", e);
    }
  }

  @Override
  public Status process() throws EventDeliveryException {
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    transaction.begin();
    try {
      int txnEventCount;
      for (txnEventCount = 0; txnEventCount < batchSize; txnEventCount++) {
        Event event = channel.take();

        String realPath = BucketPath.escapeString(filePath, event.getHeaders(),
            timeZone, needRounding, roundUnit, roundValue, useLocalTime);
        String realName = BucketPath.escapeString(fileName, event.getHeaders(),
            timeZone, needRounding, roundUnit, roundValue, useLocalTime);
        String lookupPath = realPath + DIRECTORY_DELIMITER + realName;

        HiveBatchWriter writer;
        synchronized (writersLock) {
          writer = writers.get(lookupPath);
          if (writer == null) {
            writer = initializeHiveBatchWriter(lookupPath, null, null);
            writers.put(lookupPath, writer);
          }
        }

        writer.append(event.getBody());
      }
      // FIXME data may not flush to orcfile after commit transaction, which will cause data lose
      transaction.commit();
      if (txnEventCount < 1) {
        return Status.BACKOFF;
      } else {
        return Status.READY;
      }
    } catch (Exception e) {
      transaction.rollback();
      LOG.error("process failed", e);
      throw new EventDeliveryException(e);
    } finally {
      transaction.close();
    }
  }

  private HiveBatchWriter initializeHiveBatchWriter(String realPath,
                                                    List<HiveBatchWriter.Callback> initCallbacks,
                                                    List<HiveBatchWriter.Callback> closeCallbacks)
      throws IOException {
    return new HiveBatchWriter(conf, deserializer, realPath, idleTimeout,
        initCallbacks, closeCallbacks);
  }

  @Override
  public synchronized void start() {
    this.isRunning = true;
    this.writers = new WriterLinkedHashMap(maxOpenFiles);
    this.idleWriterMonitor = new IdleHiveBatchWriterMonitor();
    new DaemonThreadFactory("IdleHiveBatchWriterMonitor").newThread(this.idleWriterMonitor).start();
    super.start();
  }

  @Override
  public synchronized void stop() {
    this.isRunning = false;
    for (Map.Entry<String, HiveBatchWriter> entry : writers.entrySet()) {
      LOG.info("Closing {}", entry.getKey());
      try {
        entry.getValue().close();
      } catch (Exception e) {
        LOG.warn("Exception while closing " + entry.getKey() + ". " +
            "Exception follows.", e);
      }
    }
    writers.clear();
    writers = null;
    super.stop();
  }

  @Override
  public String toString() {
    return "{ Sink type:" + getClass().getSimpleName() + ", name:" + getName() + " }";
  }

}
