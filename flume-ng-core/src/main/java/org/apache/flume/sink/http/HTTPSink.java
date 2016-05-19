package org.apache.flume.sink.http;

import com.google.common.base.Preconditions;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by Tao Li on 5/19/16.
 */
public class HTTPSink extends AbstractSink implements Configurable {
  private final Logger LOG = LoggerFactory.getLogger(HTTPSink.class);


  private String url;
  private int batchSize;
  private SinkCounter sinkCounter;

  @Override
  public void configure(Context context) {
    url = Preconditions.checkNotNull(context.getString("url"), "url is required");
    batchSize = context.getInteger("batchSize", 100);

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }
  }

  @Override
  public Status process() throws EventDeliveryException {
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    transaction.begin();
    try {
      int txnEventCount;
      for (txnEventCount = 0; txnEventCount < batchSize; txnEventCount++) {
        Event event = channel.take();
        if (event == null) {
          break;
        }

        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setFixedLengthStreamingMode(event.getBody().length);
        conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        conn.connect();
        OutputStream os = conn.getOutputStream();
        os.write(event.getBody());
        os.flush();
        os.close();

        if (conn.getResponseCode() != 200) {
          LOG.error("Fail to send http post");
          LOG.error("event body: " + new String(event.getBody(), "UTF-8"));
          LOG.error("response code: " + conn.getResponseCode());
          BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
          String line;
          StringBuffer response = new StringBuffer();
          while ((line = in.readLine()) != null) {
            response.append(line);
          }
          in.close();
          LOG.error("response data: " + response.toString());
        }
      }

      if (txnEventCount == 0) {
        sinkCounter.incrementBatchEmptyCount();
      } else if (txnEventCount == batchSize) {
        sinkCounter.incrementBatchCompleteCount();
      } else {
        sinkCounter.incrementBatchUnderflowCount();
      }

      transaction.commit();

      if (txnEventCount < 1) {
        return Status.BACKOFF;
      } else {
        sinkCounter.addToEventDrainSuccessCount(txnEventCount);
        return Status.READY;
      }
    } catch (IOException e) {
      transaction.rollback();
      LOG.warn("IO error", e);
      return Status.BACKOFF;
    } catch (Exception e) {
      transaction.rollback();
      LOG.error("process failed", e);
      throw new EventDeliveryException(e);
    } finally {
      transaction.close();
    }
  }

  @Override
  public synchronized void start() {
    sinkCounter.start();
    super.start();
  }

  @Override
  public synchronized void stop() {
    sinkCounter.stop();
    super.stop();
  }

  @Override
  public String toString() {
    return "{ Sink type:" + getClass().getSimpleName() + ", name:" + getName() + " }";
  }
}
