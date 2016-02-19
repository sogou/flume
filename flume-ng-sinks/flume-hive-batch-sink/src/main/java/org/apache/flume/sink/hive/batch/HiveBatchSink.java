package org.apache.flume.sink.hive.batch;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.hive.batch.callback.AddPartitionCallback;
import org.apache.flume.sink.hive.batch.util.HiveUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
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

  private Configuration conf;
  private String dbName;
  private String tableName;
  private String partition;
  private String path;
  private String fileName;
  private String suffix;
  private TimeZone timeZone;
  private int maxOpenFiles;
  private long batchSize;
  private long idleTimeout;
  private boolean needRounding;
  private int roundUnit = Calendar.SECOND;
  private int roundValue = 1;
  private boolean useLocalTime = false;
  private Deserializer deserializer;

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
                  LOG.warn("Exception while closing " + writer.getFile()
                      + ". Exception follows.", e);
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
    conf = new Configuration();
    dbName = context.getString(Config.HIVE_DATABASE, Config.Default.DEFAULT_DATABASE);
    tableName = Preconditions.checkNotNull(context.getString(Config.HIVE_TABLE),
        Config.HIVE_TABLE + " is required");
    path = Preconditions.checkNotNull(context.getString(Config.HIVE_PATH),
        Config.HIVE_PATH + " is required");
    partition = context.getString(Config.HIVE_PARTITION, Config.Default.DEFAULT_PARTITION);
    fileName = context.getString(Config.HIVE_FILE_PREFIX, Config.Default.DEFAULT_FILE_PREFIX);
    this.suffix = context.getString(Config.HIVE_FILE_SUFFIX, Config.Default.DEFAULT_FILE_SUFFIX);
    String tzName = context.getString(Config.HIVE_TIME_ZONE);
    timeZone = tzName == null ? null : TimeZone.getTimeZone(tzName);
    maxOpenFiles = context.getInteger(Config.HIVE_MAX_OPEN_FILES, Config.Default.DEFAULT_MAX_OPEN_FILES);
    batchSize = context.getLong(Config.HIVE_BATCH_SIZE, Config.Default.DEFAULT_BATCH_SIZE);
    idleTimeout = context.getLong(Config.HIVE_IDLE_TIMEOUT, Config.Default.DEFAULT_IDLE_TIMEOUT);

    String serdeName = Preconditions.checkNotNull(context.getString(Config.HIVE_SERDE),
        Config.HIVE_SERDE + " is required");
    Map<String, String> serdeProperties = context.getSubProperties(Config.HIVE_SERDE_PROPERTIES + ".");
    try {
      Properties tbl = new Properties();
      for (Map.Entry<String, String> entry : serdeProperties.entrySet()) {
        tbl.setProperty(entry.getKey(), entry.getValue());
      }

      List<FieldSchema> fields = HiveUtils.getFields(dbName, tableName);
      List<String> columnNames = new ArrayList<String>();
      List<String> columnTypes = new ArrayList<String>();
      for (FieldSchema field : fields) {
        columnNames.add(field.getName());
        columnTypes.add(field.getType());
      }
      String columnNameProperty = Joiner.on(",").join(columnNames);
      String columnTypeProperty = Joiner.on(",").join(columnTypes);
      tbl.setProperty(serdeConstants.LIST_COLUMNS, columnNameProperty);
      tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, columnTypeProperty);

      deserializer = (Deserializer) Class.forName(serdeName).newInstance();
      deserializer.initialize(conf, tbl);
    } catch (Exception e) {
      throw new IllegalArgumentException(serdeName + " init failed", e);
    }

    needRounding = context.getBoolean(Config.HIVE_ROUND, Config.Default.DEFAULT_ROUND);
    if (needRounding) {
      String unit = context.getString(Config.HIVE_ROUND_UNIT, Config.Default.DEFAULT_ROUND_UNIT);
      if (unit.equalsIgnoreCase("hour")) {
        this.roundUnit = Calendar.HOUR_OF_DAY;
      } else if (unit.equalsIgnoreCase("minute")) {
        this.roundUnit = Calendar.MINUTE;
      } else if (unit.equalsIgnoreCase("second")) {
        this.roundUnit = Calendar.SECOND;
      } else {
        LOG.warn("Rounding unit is not valid, please set one of minute, hour, or second. Rounding will be disabled");
        needRounding = false;
      }
      this.roundValue = context.getInteger(Config.HIVE_ROUND_VALUE, Config.Default.DEFAULT_ROUND_VALUE);
      if (roundUnit == Calendar.SECOND || roundUnit == Calendar.MINUTE) {
        Preconditions.checkArgument(roundValue > 0 && roundValue <= 60, "Round value must be > 0 and <= 60");
      } else if (roundUnit == Calendar.HOUR_OF_DAY) {
        Preconditions.checkArgument(roundValue > 0 && roundValue <= 24, "Round value must be > 0 and <= 24");
      }
    }
    this.useLocalTime = context.getBoolean(Config.HIVE_USE_LOCAL_TIMESTAMP, Config.Default.DEFAULT_USE_LOCAL_TIMESTAMP);
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
        if (event == null) {
          break;
        }

        String rootPath = BucketPath.escapeString(path, event.getHeaders(),
            timeZone, needRounding, roundUnit, roundValue, useLocalTime);
        String realPartition = BucketPath.escapeString(partition, event.getHeaders(),
            timeZone, needRounding, roundUnit, roundValue, useLocalTime);
        String realName = BucketPath.escapeString(fileName, event.getHeaders(),
            timeZone, needRounding, roundUnit, roundValue, useLocalTime);
        String partitionPath = rootPath + DIRECTORY_DELIMITER + realPartition;
        String keyPath = partitionPath + DIRECTORY_DELIMITER + realName;
        // FIXME fullFileName may not be unique, which will cause write to the same orc file
        String fullFileName = realName + "." + System.nanoTime() + "." + this.suffix;

        HiveBatchWriter writer;
        synchronized (writersLock) {
          writer = writers.get(keyPath);
          if (writer == null) {
            writer = initializeHiveBatchWriter(partitionPath, fullFileName, realPartition);
            writers.put(keyPath, writer);
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

  private HiveBatchWriter initializeHiveBatchWriter(String path, String fileName, String partition)
      throws IOException, SerDeException {
    String file = path + DIRECTORY_DELIMITER + fileName;

    // TODO callbacks should be configurable not hard code
    List<String> values = new ArrayList<String>();
    for (String part : partition.split("/")) {
      values.add(part.split("=")[1]);
    }
    List<HiveBatchWriter.Callback> closeCallbacks = new ArrayList<HiveBatchWriter.Callback>();
    HiveBatchWriter.Callback addPartitionCallback = new AddPartitionCallback(dbName, tableName,
        values, path);
    closeCallbacks.add(addPartitionCallback);

    return new HiveBatchWriter(conf, deserializer, file, idleTimeout, null, closeCallbacks);
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
