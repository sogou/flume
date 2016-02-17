package com.sogou.flume.sink.hive;

/**
 * Created by Tao Li on 2016/2/17.
 */
public class Config {
  public static final String HIVE_DATABASE = "hive.database";
  public static final String HIVE_TABLE = "hive.table";

  public static final String HIVE_PATH = "hive.path";
  public static final String HIVE_FILE_NAME = "hive.fileName";
  public static final String HIVE_TIME_ZONE = "hive.timeZone";
  public static final String HIVE_MAX_OPEN_FILES = "hive.maxOpenFiles";
  public static final String HIVE_BATCH_SIZE = "hive.batchSize";
  public static final String HIVE_DESERIALIZER = "hive.deserializer";

  public class Default {
    public static final String DEFAULT_DATABASE = "default";
    public static final String DEFAULT_FILE_NAME = "FlumeData";
    public static final int DEFAULT_MAX_OPEN_FILES = 5000;
    public static final long DEFAULT_BATCH_SIZE = 1000;
  }
}