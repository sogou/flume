package org.apache.flume.sink.hive.batch;

/**
 * Created by Tao Li on 2016/2/17.
 */
public class Config {
  public static final String HIVE_DATABASE = "hive.database";
  public static final String HIVE_TABLE = "hive.table";
  public static final String HIVE_PATH = "hive.path";
  public static final String HIVE_PARTITION = "hive.partition";
  public static final String HIVE_FILE_PREFIX = "hive.filePrefix";
  public static final String HIVE_FILE_SUFFIX = "hive.fileSuffix";
  public static final String HIVE_TIME_ZONE = "hive.timeZone";
  public static final String HIVE_MAX_OPEN_FILES = "hive.maxOpenFiles";
  public static final String HIVE_BATCH_SIZE = "hive.batchSize";
  public static final String HIVE_IDLE_TIMEOUT = "hive.idleTimeout";
  public static final String HIVE_CALL_TIMEOUT = "hive.callTimeout";
  public static final String HIVE_THREADS_POOL_SIZE = "hive.threadsPoolSize";
  public static final String HIVE_SERDE = "hive.serde";
  public static final String HIVE_SERDE_PROPERTIES = "hive.serdeProperties";
  public static final String HIVE_ROUND = "hive.round";
  public static final String HIVE_ROUND_UNIT = "hive.roundUnit";
  public static final String HIVE_ROUND_VALUE = "hive.roundValue";
  public static final String HIVE_USE_LOCAL_TIMESTAMP = "hive.useLocalTimeStamp";
  public static final String HIVE_SINK_COUNTER_TYPE = "hive.sinkCounterType";
  public static final String Hive_ZOOKEEPER_CONNECT = "hive.zookeeperConnect";
  public static final String HIVE_ZOOKEEPER_SESSION_TIMEOUT = "hive.zookeeperSessionTimeout";
  public static final String HIVE_ZOOKEEPER_SERVICE_NAME = "hive.zookeeperServiceName";
  public static final String HIVE_HOST_NAME = "hive.hostName";
  public static final String HIVE_DB_CONNECT_URL = "hive.dbConnectURL";

  public static class Default {
    public static final String DEFAULT_DATABASE = "default";
    public static final String DEFAULT_PARTITION = "";
    public static final String DEFAULT_FILE_PREFIX = "FlumeData";
    public static final String DEFAULT_FILE_SUFFIX = "orc";
    public static final int DEFAULT_MAX_OPEN_FILES = 5000;
    public static final long DEFAULT_BATCH_SIZE = 1000;
    public static final long DEFAULT_IDLE_TIMEOUT = 5000;
    public static final long DEFAULT_CALL_TIMEOUT = 10000;
    public static final int DEFAULT_THREADS_POOL_SIZE = 5;
    public static final boolean DEFAULT_ROUND = false;
    public static final String DEFAULT_ROUND_UNIT = "second";
    public static final int DEFAULT_ROUND_VALUE = 1;
    public static final boolean DEFAULT_USE_LOCAL_TIMESTAMP = false;
    public static final String DEFAULT_SINK_COUNTER_TYPE = "SinkCounter";
    public static final String DEFAULT_ZOOKEEPER_CONNECT = null;
    public static final int DEFAULT_ZOOKEEPER_SESSION_TIMEOUT = 5000;
    public static final String DEFAULT_DB_CONNECT_URL = null;
  }
}