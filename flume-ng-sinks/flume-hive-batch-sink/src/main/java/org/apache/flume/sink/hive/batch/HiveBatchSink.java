package org.apache.flume.sink.hive.batch;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.hive.batch.callback.AddPartitionCallback;
import org.apache.flume.sink.hive.batch.util.HiveUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

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
  private long callTimeout;
  private int threadsPoolSize;
  private boolean needRounding;
  private int roundUnit = Calendar.SECOND;
  private int roundValue = 1;
  private boolean useLocalTime = false;
  private Deserializer deserializer;

  private AtomicLong writerCounter;
  private WriterLinkedHashMap writers;
  private ExecutorService callTimeoutPool;

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
    callTimeout = context.getLong(Config.HIVE_CALL_TIMEOUT, Config.Default.DEFAULT_CALL_TIMEOUT);
    threadsPoolSize = context.getInteger(Config.HIVE_THREADS_POOL_SIZE, Config.Default.DEFAULT_THREADS_POOL_SIZE);

    String serdeName = Preconditions.checkNotNull(context.getString(Config.HIVE_SERDE),
        Config.HIVE_SERDE + " is required");
    Map<String, String> serdeProperties = context.getSubProperties(Config.HIVE_SERDE_PROPERTIES + ".");
    try {
      Properties tbl = HiveUtils.getTableColunmnProperties(dbName, tableName);
      for (Map.Entry<String, String> entry : serdeProperties.entrySet()) {
        tbl.setProperty(entry.getKey(), entry.getValue());
      }
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

        HiveBatchWriter writer = writers.get(keyPath);
        if (writer == null) {
          long counter = writerCounter.addAndGet(1);
          String fullFileName = realName + "." + System.nanoTime() + "." + counter + "." + this.suffix;
          writer = initializeHiveBatchWriter(partitionPath, fullFileName, realPartition);
          writers.put(keyPath, writer);
        }
        writer.append(event.getBody());
      }

      closeIdleWriters();

      // FIXME data may not flush to orcfile after commit transaction, which will cause data lose
      transaction.commit();

      if (txnEventCount < 1) {
        return Status.BACKOFF;
      } else {
        return Status.READY;
      }
    } catch (IOException e) {
      transaction.rollback();
      LOG.warn("Hive IO error", e);
      return Status.BACKOFF;
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

  private void closeIdleWriters() throws IOException, InterruptedException {
    Map<String, HiveBatchWriter> closeWriters = Maps.filterValues(writers,
        new Predicate<HiveBatchWriter>() {
          @Override
          public boolean apply(@Nullable HiveBatchWriter writer) {
            return !writer.isIdle();
          }
        });

    List<Callable<Void>> callables = Lists.newArrayList(Iterables.transform(closeWriters.entrySet(),
        new Function<Map.Entry<String, HiveBatchWriter>, Callable<Void>>() {
          @Override
          public Callable<Void> apply(@Nullable Map.Entry<String, HiveBatchWriter> entry) {
            final String path = entry.getKey();
            final HiveBatchWriter writer = entry.getValue();
            return new Callable<Void>() {
              @Override
              public Void call() throws Exception {
                LOG.info("Closing {}", path);
                writer.close();
                return null;
              }
            };
          }
        }));

    try {
      callWithTimeout(callables, callTimeout, callTimeoutPool);
    } catch (TimeoutException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw e;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else if (cause instanceof InterruptedException) {
        throw (InterruptedException) cause;
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else {
        throw new RuntimeException(e);
      }
    } finally {
      Maps.transformEntries(closeWriters, new Maps.EntryTransformer<String, HiveBatchWriter, Void>() {
        @Override
        public Void transformEntry(@Nullable String path, @Nullable HiveBatchWriter writer) {
          writers.remove(path);
          return null;
        }
      });
    }
  }

  private <T> List<T> callWithTimeout(List<Callable<T>> callables,
                                      long callTimeout, ExecutorService service)
      throws ExecutionException, InterruptedException, TimeoutException {
    List<Future<T>> futures = new ArrayList<Future<T>>();
    for (Callable<T> callable : callables) {
      futures.add(callTimeoutPool.submit(callable));
    }

    boolean isTimeout = false;
    List<T> results = new ArrayList<T>();
    for (Future<T> future : futures) {
      if (callTimeout > 0) {
        try {
          results.add(future.get(callTimeout, TimeUnit.MILLISECONDS));
        } catch (TimeoutException e) {
          future.cancel(true);
          isTimeout = true;
        }
      } else {
        results.add(future.get());
      }
    }

    if (isTimeout) {
      throw new TimeoutException("Callables timed out after " + callTimeout + " ms");
    }

    return results;
  }

  @Override
  public synchronized void start() {
    this.writerCounter = new AtomicLong(0);
    this.writers = new WriterLinkedHashMap(maxOpenFiles);
    String timeoutName = "hive-batch-" + getName() + "-call-runner-%d";
    this.callTimeoutPool = Executors.newFixedThreadPool(threadsPoolSize,
        new ThreadFactoryBuilder().setNameFormat(timeoutName).build());
    super.start();
  }

  @Override
  public synchronized void stop() {
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
