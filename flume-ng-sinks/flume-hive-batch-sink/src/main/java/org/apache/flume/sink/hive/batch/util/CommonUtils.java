package org.apache.flume.sink.hive.batch.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * Created by Tao Li on 2016/3/1.
 */
public class CommonUtils {
  public static String getStackTraceStr(Exception exception) {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    exception.printStackTrace(new PrintStream(stream));
    return stream.toString();
  }
}
