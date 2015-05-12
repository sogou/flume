package org.apache.flume.instrumentation.sogou;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.flume.Event;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Tao Li on 4/29/15.
 */
public class TimedUtils {
  private static final String FIVE_MIN_TIME_FOTMAT = "yyyyMMddHHmm";
  private static SimpleDateFormat fiveMinSDF = new SimpleDateFormat(FIVE_MIN_TIME_FOTMAT);

  private static final String EVENT_CATEGORY_KEY = "category";
  private static final String EVENT_TIMESTAMP_KEY = "timestamp";

  private static final String NO_CATEGORY = "no_category";
  private static final String NO_TIMESTAMP = "no_timestamp";
  private static final String INVALID_TIMESTAMP = "invalid_timestamp";

  private static final java.lang.reflect.Type FIVE_MIN_MAP_TYPE =
      new TypeToken<Map<String, TimestampCount>>() {
      }.getType();
  private static final java.lang.reflect.Type CATEGORY_FIVE_MIN_MAP_TYPE =
      new TypeToken<Map<String, Map<String, TimestampCount>>>() {
      }.getType();
  private static Gson gson = new Gson();

  @Deprecated
  public static String convertTimestampToFiveMinStr(long timestamp) {
    long fiveMinTimestamp = (long) Math.floor(timestamp / 300000) * 300000;
    return fiveMinSDF.format(new Date(fiveMinTimestamp));
  }

  @Deprecated
  public static String convertTimestampStrToFiveMinStr(String timestampStr) {
    return convertTimestampToFiveMinStr(Long.parseLong(timestampStr));
  }

  public static long convertTimestampToFiveMinTimestamp(long timestamp) {
    return (long) Math.floor(timestamp / 300000) * 300000;
  }

  public static long convertTimestampStrToFiveMinTimestamp(String timestampStr) {
    return convertTimestampToFiveMinTimestamp(Long.parseLong(timestampStr));
  }

  public static String convertFiveMinMapToJson(Map<String, TimestampCount> fiveMinMap) {
    return gson.toJson(fiveMinMap, FIVE_MIN_MAP_TYPE);
  }

  public static String convertCategoryFiveMinMapToJson(Map<String, Map<String, TimestampCount>> fiveMinMap) {
    return gson.toJson(fiveMinMap, CATEGORY_FIVE_MIN_MAP_TYPE);
  }

  public static void updateFiveMinMap(long delta, Map<String, TimestampCount> fiveMinMap) {
    long timestamp = System.currentTimeMillis();
    String fiveMin = TimedUtils.convertTimestampToFiveMinTimestamp(timestamp) + "";
    synchronized (fiveMinMap) {
      if (!fiveMinMap.containsKey(fiveMin))
        fiveMinMap.put(fiveMin, new TimestampCount());
      fiveMinMap.get(fiveMin).addToCountAndTimestamp(delta, timestamp);
    }
  }

  public static void updateCategoryFiveMinMap(List<Event> events, Map<String, Map<String, TimestampCount>> fiveMinMap) {
    if (events == null && events.size() == 0) {
      return;
    }

    Map<String, Long> counters = new HashMap<String, Long>();
    for (Event event : events) {
      Map<String, String> headers = event.getHeaders();

      String category = headers.containsKey(EVENT_CATEGORY_KEY) ?
          headers.get(EVENT_CATEGORY_KEY) : NO_CATEGORY;
      String fiveMin = NO_TIMESTAMP;
      if (headers.containsKey(EVENT_TIMESTAMP_KEY)) {
        String timestampStr = headers.get(EVENT_TIMESTAMP_KEY);
        try {
          fiveMin = TimedUtils.convertTimestampStrToFiveMinTimestamp(timestampStr) + "";
        } catch (Exception e) {
          fiveMin = INVALID_TIMESTAMP;
        }
      }

      String key = category + "\t" + fiveMin;
      if (!counters.containsKey(key)) {
        counters.put(key, 0L);
      }
      counters.put(key, counters.get(key) + 1);
    }

    long updateTimestamp = System.currentTimeMillis();

    synchronized (fiveMinMap) {
      for (Map.Entry<String, Long> entry : counters.entrySet()) {
        String[] arr = entry.getKey().split("\t");
        String category = arr[0];
        String fiveMin = arr[1];
        long num = entry.getValue();

        if (!fiveMinMap.containsKey(category)) {
          fiveMinMap.put(category, new FiveMinLinkedHashMap<String, TimestampCount>());
        }
        if (!fiveMinMap.get(category).containsKey(fiveMin)) {
          fiveMinMap.get(category).put(fiveMin, new TimestampCount());
        }

        fiveMinMap.get(category).get(fiveMin).addToCountAndTimestamp(num, updateTimestamp);
      }
    }
  }

  public static class FiveMinLinkedHashMap<String, TimestampCount> extends LinkedHashMap<String, TimestampCount> {
    private static final int DEFAULT_MAX_SIZE = 500;
    private int maxSize = DEFAULT_MAX_SIZE;

    public FiveMinLinkedHashMap(int maxSize) {
      super(16, 0.75f, false);
      this.maxSize = maxSize;
    }

    public FiveMinLinkedHashMap() {
      this(DEFAULT_MAX_SIZE);
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, TimestampCount> eldest) {
      if (size() > maxSize) {
        return true;
      } else {
        return false;
      }
    }
  }

  public static class TimestampCount {
    private long count;
    private long timestamp;

    public TimestampCount() {
      this(0, System.currentTimeMillis());
    }

    public TimestampCount(long count, long timestamp) {
      this.count = count;
      this.timestamp = timestamp;
    }

    public long addToCountAndTimestamp(long delta, long timestamp) {
      this.count += delta;
      this.timestamp = timestamp;
      return this.count;
    }
  }
}
