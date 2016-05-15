package org.apache.flume.sink.hive.batch.dao;

import com.google.common.base.Joiner;
import org.apache.flume.sink.hive.batch.util.DBManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tao Li on 2016/3/2.
 */
public class HiveSinkDetailDao {
  private Logger LOG = LoggerFactory.getLogger(HiveSinkDetailDao.class);

  private final String TABLE_NAME = "hive_sink_detail";

  private String name;
  private DBManager dbManager;

  public HiveSinkDetailDao(String connectURL, String name) {
    this.dbManager = new DBManager(connectURL);
    this.name = name;
  }

  public void connect() throws SQLException {
    dbManager.connect();
  }

  public void close() throws SQLException {
    dbManager.close();
  }

  public List<String[]> getFinishedList(int onlineServerNum) throws SQLException {
    String sql = String.format(
        "SELECT t.`partition` AS `partition`, t.location AS location FROM("
            + "SELECT `partition`, location, COUNT(*) AS n FROM %s "
            + "WHERE state='NEW' AND name='%s' AND partition is not null AND location is not null "
            + "GROUP BY `partition`, location) t "
            + "WHERE t.n >= %s",
        TABLE_NAME, name, onlineServerNum
    );
    ResultSet rs = dbManager.executeQuery(sql);
    List<String[]> list = new ArrayList<String[]>();
    try {
      while (rs.next()) {
        list.add(new String[]{rs.getString("partition"), rs.getString("location")});
      }
    } finally {
      if (rs != null) {
        rs.close();
      }
    }
    return list;
  }

  public void updateCheckedState(List<String> partitionList) throws SQLException {
    if (partitionList.size() == 0) {
      return;
    }
    String partitionSQL = "'" + Joiner.on("', '").join(partitionList) + "'";
    String sql = String.format(
        "UPDATE %s SET state='CHECKED' WHERE state='NEW' AND name='%s' AND partition in (%s)",
        TABLE_NAME, name, partitionSQL
    );
    dbManager.execute(sql);
  }

  public boolean exists(String partition, String hostName) throws SQLException {
    String sql = String.format(
        "SELECT * FROM %s WHERE name='%s' AND `partition`='%s' AND hostname='%s'",
        TABLE_NAME, this.name, partition, hostName
    );
    ResultSet rs = dbManager.executeQuery(sql);
    try {
      return rs.next();
    } finally {
      if (rs != null)
        rs.close();
    }
  }

  public void create(String partition, String location, String hostName,
                     long receiveCount, long sinkCount, long updateTimestamp) throws SQLException {
    String sql = String.format(
        "INSERT INTO %s(name, `partition`, location, hostname, receivecount, sinkcount, updatetime) "
            + "VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s')",
        TABLE_NAME, name, partition, location, hostName, receiveCount, sinkCount, updateTimestamp);
    dbManager.execute(sql);
  }

  public void update(String partition, String location, String hostName,
                     long receiveCount, long sinkCount, long updateTimestamp) throws SQLException {
    String sql = String.format(
        "UPDATE %s SET receivecount='%s', sinkcount='%s', updatetime='%s' "
            + "WHERE name='%s' AND `partition`='%s' AND location='%s' AND hostname='%s'",
        TABLE_NAME, receiveCount, sinkCount, updateTimestamp, name, partition, location, hostName
    );
    dbManager.execute(sql);
  }
}
