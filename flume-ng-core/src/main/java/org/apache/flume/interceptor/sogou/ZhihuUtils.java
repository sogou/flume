package org.apache.flume.interceptor.sogou;

/**
 * Created by Tao Li on 2015/8/17.
 */
public class ZhihuUtils {
  public static final String ZHIHU_PUSH_FREQ_FLAG = "freq";
  public static final String ZHIHU_PUSH_FAST_FLAG = "fast";
  public static final String ZHIHU_PUSH_INSTANT_FLAG = "instant";

  public static final String ZHIHU_PUSH_FREQ_TYPE = "freq";
  public static final String ZHIHU_PUSH_FAST_TYPE = "fast";
  public static final String ZHIHU_PUSH_INSTANT_TYPE = "instant";
  public static final String ZHIHU_PUSH_UNKNOWN_TYPE = "unknown";

  public static String getPushTypeFromFileName(String fileName) {
    if (fileName != null) {
      fileName = fileName.toLowerCase();
      if (fileName.contains(ZHIHU_PUSH_FREQ_FLAG))
        return ZHIHU_PUSH_FREQ_TYPE;
      else if (fileName.contains(ZHIHU_PUSH_FAST_FLAG))
        return ZHIHU_PUSH_FAST_TYPE;
      else if (fileName.contains(ZHIHU_PUSH_INSTANT_FLAG))
        return ZHIHU_PUSH_INSTANT_TYPE;
    }
    return ZHIHU_PUSH_UNKNOWN_TYPE;
  }
}
