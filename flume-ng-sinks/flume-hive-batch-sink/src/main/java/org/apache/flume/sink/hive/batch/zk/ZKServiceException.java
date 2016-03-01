package org.apache.flume.sink.hive.batch.zk;

/**
 * Created by Tao Li on 2/5/15.
 */
public class ZKServiceException extends Exception {
  public ZKServiceException() {
    super();
  }

  public ZKServiceException(String message) {
    super(message);
  }

  public ZKServiceException(Throwable cause) {
    super(cause);
  }

  public ZKServiceException(String message, Throwable cause) {
    super(message, cause);
  }
}
