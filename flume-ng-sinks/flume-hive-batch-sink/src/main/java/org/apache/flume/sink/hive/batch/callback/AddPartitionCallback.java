package org.apache.flume.sink.hive.batch.callback;

import org.apache.flume.sink.hive.batch.HiveBatchWriter;
import org.apache.flume.sink.hive.batch.util.HiveUtils;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by Tao Li on 2016/2/18.
 */
public class AddPartitionCallback implements HiveBatchWriter.Callback {
  private static final Logger LOG = LoggerFactory.getLogger(HiveBatchWriter.class);

  private String dbName;
  private String tableName;
  private List<String> values;
  private String location;

  public AddPartitionCallback(String dbName, String tableName,
                              List<String> values, String location) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.values = values;
    this.location = location;
  }

  @Override
  public void run() {
    try {
      HiveUtils.addPartition(dbName, tableName, values, location);
    } catch (AlreadyExistsException e) {
      LOG.warn("Partition already exists: " + dbName + "." + tableName + " " + values);
    } catch (TException e) {
      LOG.error("Fail to add partition: " + dbName + "." + tableName + " " + values, e);
    }
  }
}
