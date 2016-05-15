package org.apache.flume.sink.hive.batch.callback;

import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.instrumentation.sogou.TimedSinkCounter;
import org.apache.flume.sink.hive.batch.HiveBatchWriter;
import org.apache.flume.sink.hive.batch.dao.HiveSinkDetailDao;
import org.apache.flume.sink.hive.batch.util.CommonUtils;
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
  private final String logdate;
  private final String hostName;
  private final String partition;
  private final String location;

  private TimedSinkCounter sinkCounter = null;

  public UpdateSinkDetailCallback(String connectURL, String name, String logdate, String hostName,
                                  String partition, String location,
                                  SinkCounter sinkCounter) {
    this.connectURL = connectURL;
    this.name = name;
    this.logdate = logdate;
    this.hostName = hostName;
    this.partition = partition;
    this.location = location;
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
      if (sinkCounter != null &&
          sinkCounter.getEventDrainSuccessCountInFiveMinMap().containsKey(logdate)) {
        sinkCount = sinkCounter.getEventDrainSuccessCountInFiveMinMap().get(logdate).getCount();
      }
      long updateTimestamp = System.currentTimeMillis();
      if (dao.exists(logdate, hostName)) {
        dao.update(logdate, hostName, receiveCount, sinkCount, updateTimestamp, partition, location);
      } else {
        dao.create(logdate, hostName, receiveCount, sinkCount, updateTimestamp, partition, location);
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
