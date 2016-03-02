package org.apache.flume.sink.hive.batch.util;

import java.sql.*;

/**
 * Created by Tao Li on 2016/3/2.
 */
public class DBManager {
  private final String connectURL;
  private Connection conn = null;
  private Statement stmt = null;

  public DBManager(String connectURL) {
    this.connectURL = connectURL;
  }

  public void connect() throws SQLException {
    conn = DriverManager.getConnection(this.connectURL);
    stmt = conn.createStatement();
  }

  public void execute(String sql) throws SQLException {
    stmt.execute(sql);
  }

  public ResultSet executeQuery(String sql) throws SQLException {
    return stmt.executeQuery(sql);
  }

  public void close() throws SQLException {
    if (conn != null) {
      conn.close();
    }
  }
}
