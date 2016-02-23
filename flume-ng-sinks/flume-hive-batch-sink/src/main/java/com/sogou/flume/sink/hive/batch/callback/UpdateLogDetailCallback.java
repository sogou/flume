package com.sogou.flume.sink.hive.batch.callback;

import org.apache.flume.sink.hive.batch.HiveBatchWriter;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Tao Li on 2016/2/18.
 */
public class UpdateLogDetailCallback implements HiveBatchWriter.Callback {
  private static final Logger LOG = LoggerFactory.getLogger(UpdateLogDetailCallback.class);

  private String rootUrl;
  private int logId;
  private String time;

  public UpdateLogDetailCallback(String rootUrl, int logId, String time) {
    this.rootUrl = rootUrl;
    this.logId = logId;
    this.time = time;
  }

  @Override
  public void run() {
    String url = String.format("%s/%d/%s", rootUrl, logId, time);
    String entity = "";

    try {
      HttpPost request = new HttpPost(url);
      request.setEntity(new StringEntity(entity));
      HttpClient httpClient = new DefaultHttpClient();
      HttpResponse response = httpClient.execute(request);
      int statusCode = response.getStatusLine().getStatusCode();

      if (statusCode != HttpStatus.SC_OK) {
        LOG.error("fail to update logdetail, status code:" + statusCode);
      }
    } catch (Exception e) {
      LOG.error("fail to update logdetail", e);
    }
  }

  public static void main(String[] args) {
    String rootUrl = args[0];
    int logId = Integer.parseInt(args[1]);
    String time = args[2];
    new UpdateLogDetailCallback(rootUrl, logId, time).run();
  }
}