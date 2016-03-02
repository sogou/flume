package org.apache.flume.sink.hive.batch.util;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Tao Li on 2016/3/2.
 */
public class DTEUtils {
  private static Logger LOG = LoggerFactory.getLogger(DTEUtils.class);

  public static void updateLogDetail(String serviceURL, int logid, String logdate) {
    String url = String.format("%s/%s/%s", serviceURL, logid, logdate);
    try {
      HttpPost request = new HttpPost(url);
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
}
