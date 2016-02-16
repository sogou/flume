package com.sogou.flume.sink.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Tao Li on 2/16/16.
 */
public class HiveUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HiveBatchWriter2.class);

  public static void addPartition(Partition partition) throws TException {
    HiveConf hiveConf = new HiveConf();
    HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
    List<Partition> partitions = client.listPartitions(
        partition.getDbName(), partition.getTableName(), partition.getValues(), (short) 1);
    if (partitions.size() != 0) {
      LOG.info(String.format("partition already exist: %s.%s, %s",
          partition.getDbName(), partition.getTableName(), partition.getValues()));
    } else {
      client.add_partition(partition);
    }
    client.close();
  }

  public static void addPartition(String dbName, String tableName,
                                  String columnNameProperty, String columnTypeProperty,
                                  List<String> values, String location) throws TException {
    int createTime = (int) (System.currentTimeMillis() / 1000);
    int lastAccessTime = 0;
    Map<String, String> parameters = new HashMap<String, String>();

    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
    List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    assert columnNames.size() == columnTypes.size();
    for (int i = 0; i < columnNames.size(); i++) {
      FieldSchema fieldSchema =
          new FieldSchema(columnNames.get(i), columnTypes.get(i).getTypeName(), "");
      cols.add(fieldSchema);
    }
    String inputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
    String outputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
    boolean compressed = false;
    int numBuckets = -1;
    Map<String, String> serDeInfoParameters = new HashMap<String, String>();
    serDeInfoParameters.put("serialization.format", "1");
    SerDeInfo serDeInfo = new SerDeInfo(null, "org.apache.hadoop.hive.ql.io.orc.OrcSerde", serDeInfoParameters);
    List<String> bucketCols = new ArrayList<String>();
    List<Order> sortCols = new ArrayList<Order>();
    Map<String, String> sdParameters = new HashMap<String, String>();
    StorageDescriptor sd = new StorageDescriptor(cols, location, inputFormat, outputFormat,
        compressed, numBuckets, serDeInfo, bucketCols, sortCols, sdParameters);

    Partition partition = new Partition(values, dbName, tableName, createTime, lastAccessTime, sd, parameters);
    addPartition(partition);
  }
}
