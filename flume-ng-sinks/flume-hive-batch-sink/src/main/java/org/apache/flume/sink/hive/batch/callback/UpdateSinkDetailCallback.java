package org.apache.flume.sink.hive.batch.callback;

import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.instrumentation.sogou.TimedSinkCounter;
import org.apache.flume.sink.hive.batch.HiveBatchWriter;
import org.apache.flume.sink.hive.batch.dao.HiveSinkDetailDao;
import org.apache.flume.sink.hive.batch.util.CommonUtils;
import org.apache.flume.sink.hive.batch.util.HiveUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Created by Tao Li on 2016/3/2.
 */
public class UpdateSinkDetailCallback implements HiveBatchWriter.Callback {
  private Logger LOG = LoggerFactory.getLogger(UpdateSinkDetailCallback.class);

  private final String connectURL;
  private final String name;
  private final String hostName;
  private final String partition;
  private final String location;

  private TimedSinkCounter sinkCounter = null;

  public UpdateSinkDetailCallback(String connectURL, String name, String partition,
                                  String location, String hostName, SinkCounter sinkCounter) {
    this.connectURL = connectURL;
    this.name = name;
    this.partition = partition;
    this.location = location;
    this.hostName = hostName;
    if (sinkCounter instanceof TimedSinkCounter) {
      this.sinkCounter = (TimedSinkCounter) sinkCounter;
    }
  }

  @Override
  public void run() {
    HiveSinkDetailDao dao = new HiveSinkDetailDao(connectURL, name);
    try {
      dao.connect();
      long receiveCount = 0;
      long sinkCount = 0;
      String logdate = HiveUtils.getPartitionValue(partition, "logdate");
      if (sinkCounter != null &&
          sinkCounter.getEventDrainSuccessCountInFiveMinMap().containsKey(logdate)) {
        sinkCount = sinkCounter.getEventDrainSuccessCountInFiveMinMap().get(logdate).getCount();
      }
      long updateTimestamp = System.currentTimeMillis();
      if (dao.exists(partition, hostName)) {
        dao.update(partition, location, hostName, receiveCount, sinkCount, updateTimestamp);
      } else {
        dao.create(partition, location, hostName, receiveCount, sinkCount, updateTimestamp);
      }
    } catch (SQLException e) {
      LOG.error(CommonUtils.getStackTraceStr(e));
    } finally {
      try {
        dao.close();
      } catch (SQLException e) {
        LOG.error("Fail to close dbManager", e);
      }
    }
  }
}
