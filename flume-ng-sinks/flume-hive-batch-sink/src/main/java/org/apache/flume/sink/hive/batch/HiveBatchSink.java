package org.apache.flume.sink.hive.batch;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.instrumentation.sogou.TimedSinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.hive.batch.callback.UpdateSinkDetailCallback;
import org.apache.flume.sink.hive.batch.dao.HiveSinkDetailDao;
import org.apache.flume.sink.hive.batch.util.CommonUtils;
import org.apache.flume.sink.hive.batch.util.DTEUtils;
import org.apache.flume.sink.hive.batch.util.HiveUtils;
import org.apache.flume.sink.hive.batch.zk.ZKService;
import org.apache.flume.sink.hive.batch.zk.ZKServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
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

  private volatile boolean isRunning = false;

  // Hive
  private String dbName;
  private String tableName;
  private String partition;
  private String path;
  private String fileName;
  private String suffix;
  private TimeZone timeZone;
  private Deserializer deserializer;

  // Transaction
  private int batchSize;

  // Rounding
  private boolean needRounding;
  private int roundUnit = Calendar.SECOND;
  private int roundValue = 1;
  private boolean useLocalTime = false;

  // Active writers
  private int maxOpenFiles;
  private AtomicLong writerCounter;
  private WriterLinkedHashMap activeWriters;
  private Object writersLock = new Object();

  // Idle writers
  private long idleTimeout;
  private int idleQueueSize;
  private int idleWriterCloseThreadPoolSize;
  private IdleWriterRemoveThread idleWriterRemoveThread;
  private BlockingQueue<HiveBatchWriter> idleWriters;
  private ExecutorService idleWriterCloseThreadPool;

  // Counter
  private String timedSinkCounterCategoryKey = "category";
  private String sinkCounterType = "SinkCounter";
  private SinkCounter sinkCounter;

  // Zookeeper
  private String zookeeperConnect;
  private int zookeeperSessionTimeout;
  private String zookeeperServiceName;
  private String hostName;
  private ZKService zkService = null;

  // DTE
  private String dbConnectURL;
  private String updateLogDetailURL;
  private int logId;
  private String logdateFormat;
  private LeaderThread leaderThread;

  private class WriterLinkedHashMap extends LinkedHashMap<String, HiveBatchWriter> {
    private final int maxOpenFiles;

    public WriterLinkedHashMap(int maxOpenFiles) {
      super(16, 0.75f, true);
      this.maxOpenFiles = maxOpenFiles;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, HiveBatchWriter> eldest) {
      synchronized (writersLock) {
        if (this.size() > maxOpenFiles) {
          try {
            idleWriters.put(eldest.getValue());
          } catch (InterruptedException e) {
            LOG.warn("interrupted", e);
          }
          return true;
        } else {
          return false;
        }
      }
    }
  }

  private class IdleWriterRemoveThread implements Runnable {
    private final long CHECK_INTERVAL = 5;

    @Override
    public void run() {
      while (isRunning && !Thread.currentThread().isInterrupted()) {
        synchronized (writersLock) {
          Iterator<Map.Entry<String, HiveBatchWriter>> it = activeWriters.entrySet().iterator();
          while (it.hasNext()) {
            Map.Entry<String, HiveBatchWriter> entry = it.next();
            if (entry.getValue().isIdle()) {
              try {
                // put writer to idleWriters
                idleWriters.put(entry.getValue());
              } catch (InterruptedException e) {
                LOG.warn("interrupted", e);
                Thread.currentThread().interrupt();
              }
              // remove writer from activeWriters
              it.remove();
            }
          }
        }

        try {
          TimeUnit.SECONDS.sleep(CHECK_INTERVAL);
        } catch (InterruptedException e) {
          LOG.warn("interrupted", e);
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private class IdleWriterCloseThread implements Runnable {

    @Override
    public void run() {
      while (isRunning && !Thread.currentThread().isInterrupted()) {
        try {
          HiveBatchWriter writer = idleWriters.take();
          LOG.info("Closing " + writer.getFile());
          try {
            writer.close();
          } catch (Exception e) {
            LOG.error("Fail to close " + writer.getFile(), e);
          }
        } catch (InterruptedException e) {
          LOG.warn("interrupted", e);
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private class LeaderThread implements Runnable {
    private final long CHECK_INTERVAL = 5;

    @Override
    public void run() {
      while (isRunning && !Thread.currentThread().isInterrupted()) {
        try {
          ZKService.ServerInfo leader = zkService.getLeader();
          if (leader != null && leader.getHostName().equals(hostName)) {
            // do some leader job
            int onlineServerNum = zkService.getAllServerInfos().size();
            doLeaderTask(onlineServerNum);
          }
        } catch (Exception e) {
          LOG.error(CommonUtils.getStackTraceStr(e));
        }

        try {
          TimeUnit.SECONDS.sleep(CHECK_INTERVAL);
        } catch (InterruptedException e) {
          LOG.warn("interrupted", e);
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private void doLeaderTask(int onlineServerNum) {
    HiveSinkDetailDao dao = new HiveSinkDetailDao(dbConnectURL, zookeeperServiceName);
    List<String[]> finishedList = null;
    try {
      dao.connect();
      finishedList = dao.getFinishedList(onlineServerNum);
    } catch (SQLException e) {
      LOG.error(CommonUtils.getStackTraceStr(e));
    } finally {
      try {
        dao.close();
      } catch (SQLException e) {
        LOG.error(CommonUtils.getStackTraceStr(e));
      }
    }

    if (finishedList == null || finishedList.size() == 0) {
      return;
    }

    List<String> checkedPartitionList = new ArrayList<String>();
    for (String[] info : finishedList) {
      String partition = info[0];
      String location = info[1];

      List<String> values = HiveUtils.getPartitionValues(partition);
      try {
        LOG.info("Add hive partition: " + dbName + "." + tableName + " " + values);
        addHivePartitionWithRetry(values, location);
      } catch (Exception e) {
        LOG.error("Fail to add partition: " + dbName + "." + tableName + " " + values, e);
        continue;
      }

      String logdate = HiveUtils.getPartitionValue(partition, "logdate");
      try {
        LOG.info("Update DTE LogDetail, logid: " + logId + ", logdate: " + logdate);
        DTEUtils.updateLogDetail(updateLogDetailURL, logId, logdate);
      } catch (Exception e) {
        LOG.error("Fail to update DTE LogDetail (" + logId + ", " + logdate + ")", e);
        continue;
      }

      checkedPartitionList.add(partition);
    }


    if (checkedPartitionList.size() == 0) {
      return;
    }

    try {
      dao.connect();
      dao.updateCheckedState(checkedPartitionList);
    } catch (SQLException e) {
      LOG.error(CommonUtils.getStackTraceStr(e));
    } finally {
      try {
        dao.close();
      } catch (SQLException e) {
        LOG.error(CommonUtils.getStackTraceStr(e));
      }
    }
  }

  private void addHivePartitionWithRetry(List<String> values, String location) throws Exception {
    final int MAX_RETRY_NUM = 5;
    final int SLEEP_INTERVAL = 1;
    boolean finished = false;

    for (int i = 0; i < MAX_RETRY_NUM; i++) {
      try {
        HiveUtils.addPartition(dbName, tableName, values, location);
        finished = true;
        break;
      } catch (AlreadyExistsException e) {
        LOG.warn("Partition already exists: " + dbName + "." + tableName + " " + values);
        finished = true;
        break;
      } catch (TException e) {
        LOG.error("Fail to add partition: " + dbName + "." + tableName + " " + values, e);
      }

      try {
        TimeUnit.SECONDS.sleep(SLEEP_INTERVAL);
      } catch (InterruptedException e) {
        LOG.warn("interrupted", e);
      }
    }

    if (!finished) {
      throw new Exception("Fail to add partition: " + dbName + "." + tableName + " " + values
          + " after " + MAX_RETRY_NUM + " times");
    }
  }

  @Override
  public void configure(Context context) {
    conf = new Configuration();
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);

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

    batchSize = context.getInteger(Config.HIVE_BATCH_SIZE, Config.Default.DEFAULT_BATCH_SIZE);

    idleTimeout = context.getLong(Config.HIVE_IDLE_TIMEOUT, Config.Default.DEFAULT_IDLE_TIMEOUT);
    idleQueueSize = context.getInteger(Config.HIVE_IDLE_QUEUE_SIZE, Config.Default.DEFAULT_IDLE_QUEUE_SZIE);
    idleWriterCloseThreadPoolSize = context.getInteger(Config.HIVE_IDLE_CLOSE_THREAD_POOL_SIZE, Config.Default.DEFAULT_IDLE_CLOSE_THREAD_POOL_SIZE);

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

    if (sinkCounter == null) {
      sinkCounterType = context.getString(Config.HIVE_SINK_COUNTER_TYPE, Config.Default.DEFAULT_SINK_COUNTER_TYPE);
      if (sinkCounterType.equals("TimedSinkCounter")) {
        sinkCounter = new TimedSinkCounter(getName());
        timedSinkCounterCategoryKey = context.getString("timedSinkCounterCategoryKey", "category");
      } else {
        sinkCounter = new SinkCounter(getName());
      }
    }

    this.zookeeperConnect = context.getString(Config.Hive_ZOOKEEPER_CONNECT, Config.Default.DEFAULT_ZOOKEEPER_CONNECT);
    this.zookeeperSessionTimeout = context.getInteger(Config.HIVE_ZOOKEEPER_SESSION_TIMEOUT, Config.Default.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT);
    if (this.zookeeperConnect != null) {
      this.zookeeperServiceName = context.getString(Config.HIVE_ZOOKEEPER_SERVICE_NAME);
      if (this.zookeeperServiceName == null) {
        this.zookeeperServiceName = this.dbName + "." + this.tableName;
      }
      this.hostName = Preconditions.checkNotNull(context.getString(Config.HIVE_HOST_NAME),
          Config.HIVE_HOST_NAME + " is required");

      this.dbConnectURL = context.getString(Config.HIVE_DB_CONNECT_URL, Config.Default.DEFAULT_DB_CONNECT_URL);
      if (this.dbConnectURL != null) {
        this.updateLogDetailURL = Preconditions.checkNotNull(context.getString(Config.HIVE_DTE_UPDATE_LOGDETAIL_URL),
            Config.HIVE_DTE_UPDATE_LOGDETAIL_URL + " is required");
        this.logId = Preconditions.checkNotNull(context.getInteger(Config.HIVE_DTE_LOGID),
            Config.HIVE_DTE_LOGID + " is required");
        this.logdateFormat = context.getString(Config.HIVE_DTE_LOGDATE_FORMAT, Config.Default.DEFAULT_DTE_LOGDATE_FORMAT);
      }
    }
  }

  @Override
  public Status process() throws EventDeliveryException {
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    // TODO no need to store all the events content in array list, which will increase memory usage
    List<Event> events = new ArrayList<Event>();
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

        synchronized (writersLock) {
          HiveBatchWriter writer = activeWriters.get(keyPath);
          if (writer == null) {
            long counter = writerCounter.addAndGet(1);
            String fullFileName = realName + "." + System.nanoTime() + "." + counter + "." + this.suffix;
            writer = initializeHiveBatchWriter(partitionPath, fullFileName, realPartition);
            activeWriters.put(keyPath, writer);
          }
          writer.append(event.getBody());
        }
        events.add(event);
      }

      if (txnEventCount == 0) {
        sinkCounter.incrementBatchEmptyCount();
      } else if (txnEventCount == batchSize) {
        sinkCounter.incrementBatchCompleteCount();
      } else {
        sinkCounter.incrementBatchUnderflowCount();
      }

      // FIXME data may not flush to orcfile after commit transaction, which will cause data lose
      transaction.commit();

      if (txnEventCount < 1) {
        return Status.BACKOFF;
      } else {
        sinkCounter.addToEventDrainSuccessCount(txnEventCount);
        if (sinkCounterType.equals("TimedSinkCounter")) {
          ((TimedSinkCounter) sinkCounter).addToEventDrainSuccessCountInFiveMinMap(txnEventCount);
          ((TimedSinkCounter) sinkCounter).addToCategoryEventDrainSuccessCountInFiveMinMap(events, timedSinkCounterCategoryKey);
        }
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

  private HiveBatchWriter initializeHiveBatchWriter(String location, String fileName, String partition)
      throws IOException, SerDeException {
    String file = location + DIRECTORY_DELIMITER + fileName;
    String logdate = HiveUtils.getPartitionValue(partition, "logdate");
    List<HiveBatchWriter.Callback> closeCallbacks = new ArrayList<HiveBatchWriter.Callback>();

    if (dbConnectURL != null) {
      HiveBatchWriter.Callback updateSinkDetailCallback = new UpdateSinkDetailCallback(
          dbConnectURL, zookeeperServiceName, partition, location, hostName, sinkCounter);
      closeCallbacks.add(updateSinkDetailCallback);
    }

    HiveBatchWriter writer = new HiveBatchWriter(conf, deserializer, file);
    writer.setIdleTimeout(idleTimeout);
    writer.setCloseCallbacks(closeCallbacks);
    if (logdate != null && logdateFormat != null) {
      try {
        long minFinishedTimestamp = CommonUtils.convertTimeStringToTimestamp(logdate, logdateFormat)
            + CommonUtils.getMillisecond(roundValue, roundUnit);
        writer.setMinFinishedTimestamp(minFinishedTimestamp);
      } catch (ParseException e) {
        LOG.error(CommonUtils.getStackTraceStr(e));
      }
    }

    return writer;
  }

  @Override
  public synchronized void start() {
    this.isRunning = true;
    this.writerCounter = new AtomicLong(0);
    this.activeWriters = new WriterLinkedHashMap(maxOpenFiles);
    this.idleWriters = new ArrayBlockingQueue<HiveBatchWriter>(idleQueueSize, true);
    this.idleWriterCloseThreadPool = Executors.newFixedThreadPool(idleWriterCloseThreadPoolSize,
        new ThreadFactoryBuilder().setNameFormat("idleWriterCloseThread-%d").build());
    for (int i = 0; i < idleWriterCloseThreadPoolSize; i++) {
      idleWriterCloseThreadPool.submit(new IdleWriterCloseThread());
    }
    this.idleWriterRemoveThread = new IdleWriterRemoveThread();
    new Thread(this.idleWriterRemoveThread, "IdleWriterCleanThread").start();
    sinkCounter.start();
    if (this.zookeeperConnect != null) {
      try {
        startZKServiceWithRetry();
      } catch (Exception e) {
        LOG.error("Fail to start ZKService", e);
        stop();
        return;
      }

      if (this.dbConnectURL != null) {
        // To Update DTE LogDetail
        this.leaderThread = new LeaderThread();
        new Thread(this.leaderThread, "HiveBatchSinkLeaderThread").start();
      }
    }
    super.start();
  }

  private void startZKServiceWithRetry() throws Exception {
    final int MAX_RETRY_NUM = 5;
    final int SLEEP_INTERVAL = 1;
    boolean started = false;

    for (int i = 0; i < MAX_RETRY_NUM; i++) {
      try {
        zkService = new ZKService(zookeeperConnect, zookeeperServiceName, hostName,
            zookeeperSessionTimeout);
        zkService.start();
        started = true;
        break;
      } catch (ZKServiceException e) {
        LOG.error("Fail to start ZKService", e);
      }

      try {
        TimeUnit.SECONDS.sleep(SLEEP_INTERVAL);
      } catch (InterruptedException e) {
        LOG.warn("interrupted", e);
      }
    }

    if (!started) {
      throw new Exception("Fail to start ZKService after " + MAX_RETRY_NUM + " times");
    }
  }

  @Override
  public synchronized void stop() {
    for (Map.Entry<String, HiveBatchWriter> entry : activeWriters.entrySet()) {
      LOG.info("Closing {}", entry.getKey());
      try {
        entry.getValue().close();
      } catch (Exception e) {
        LOG.warn("Exception while closing " + entry.getKey() + ". " +
            "Exception follows.", e);
      }
    }
    activeWriters.clear();
    activeWriters = null;
    sinkCounter.stop();
    if (this.zkService != null) {
      try {
        this.zkService.stop();
      } catch (ZKServiceException e) {
        LOG.error("Fail to stop ZKService", e);
      }
    }
    this.isRunning = false;
    super.stop();
  }

  @Override
  public String toString() {
    return "{ Sink type:" + getClass().getSimpleName() + ", name:" + getName() + " }";
  }

}
