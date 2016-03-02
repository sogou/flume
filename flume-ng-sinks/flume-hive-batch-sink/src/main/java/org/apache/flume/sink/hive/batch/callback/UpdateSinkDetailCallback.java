package org.apache.flume.sink.hive.batch.callback;

import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.instrumentation.sogou.TimedSinkCounter;
import org.apache.flume.sink.hive.batch.HiveBatchWriter;
import org.apache.flume.sink.hive.batch.util.CommonUtils;
import org.apache.flume.sink.hive.batch.util.DBManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by Tao Li on 2016/3/2.
 */
public class UpdateSinkDetailCallback implements HiveBatchWriter.Callback {
  private Logger LOG = LoggerFactory.getLogger(UpdateSinkDetailCallback.class);

  private final String TABLE_NAME = "hive_sink_detail";

  private final String connectURL;
  private final String name;
  private final String logdate;
  private final String hostName;

  private TimedSinkCounter sinkCounter = null;
  private DBManager dbManager = null;

  public UpdateSinkDetailCallback(String connectURL, String name, String logdate, String hostName,
                                  SinkCounter sinkCounter) {
    this.connectURL = connectURL;
    this.name = name;
    this.logdate = logdate;
    this.hostName = hostName;
    if (sinkCounter instanceof TimedSinkCounter) {
      this.sinkCounter = (TimedSinkCounter) sinkCounter;
    }
  }

  @Override
  public void run() {
    dbManager = new DBManager(connectURL);
    try {
      dbManager.connect();
      long receiveCount = 0;
      long sinkCount = 0;
      if (sinkCounter != null &&
          sinkCounter.getEventDrainSuccessCountInFiveMinMap().containsKey(logdate)) {
        sinkCount = sinkCounter.getEventDrainSuccessCountInFiveMinMap().get(logdate).getCount();
      }
      long updateTimestamp = System.currentTimeMillis();
      String sql;
      if (exists(this.name, this.logdate, this.hostName)) {
        sql = String.format(
            "UPDATE %s SET receivecount='%s', sinkcount='%s', updatetime='%s' "
                + "WHERE name='%s' AND logdate='%s' AND hostname='%s'",
            TABLE_NAME, receiveCount, sinkCount, updateTimestamp,
            this.name, this.logdate, this.hostName
        );
      } else {
        sql = String.format(
            "INSERT INTO %s(name, logdate, hostname, receivecount, sinkcount, updatetime) "
                + "VALUES('%s', '%s', '%s', '%s', '%s', '%s')",
            TABLE_NAME,
            this.name, this.logdate, this.hostName, receiveCount, sinkCount, updateTimestamp);
      }
      dbManager.execute(sql);
    } catch (SQLException e) {
      LOG.error(CommonUtils.getStackTraceStr(e));
    } finally {
      try {
        if (dbManager != null) {
          dbManager.close();
        }
      } catch (SQLException e) {
        LOG.error("Fail to close dbManager", e);
      }
    }
  }

  private boolean exists(String name, String logdate, String hostName) throws SQLException {
    String sql = String.format(
        "SELECT * FROM %s WHERE name='%s' AND logdate='%s' AND hostname='%s'",
        TABLE_NAME, this.name, this.logdate, this.hostName
    );
    ResultSet rs = dbManager.executeQuery(sql);
    try {
      return rs.next();
    } finally {
      if (rs != null)
        rs.close();
    }
  }
}
