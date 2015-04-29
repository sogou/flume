package org.apache.flume.instrumentation.sogou;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.flume.Event;
import org.apache.flume.instrumentation.SourceCounter;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Tao Li on 4/29/15.
 */
public class TimedSourceCounter extends SourceCounter implements TimedSourceCounterMBean {
  private static Gson gson = new Gson();

  private Map<String, Map<String, Long>> eventAcceptedCountInFiveMinMap = new HashMap<String, Map<String, Long>>();
  private static final java.lang.reflect.Type EVENT_ACCEPTED_COUNT_IN_FIVE_MIN_MAP_TYPE =
          new TypeToken<Map<String, Map<String, Long>>>() {
          }.getType();

  private static final String COUNTER_EVENTS_ACCEPTED_IN_FIVE_MIN =
          "src.events.accepted.5min";

  private static final String EVENT_CATEGORY_KEY = "category";
  private static final String EVENT_TIMESTAMP_KEY = "timestamp";

  private static final String NO_CATEGORY = "no_category";
  private static final String NO_TIMESTAMP = "no_timestamp";
  private static final String INVALID_TIMESTAMP = "invalid_timestamp";

  private static final String[] ATTRIBUTES =
          {COUNTER_EVENTS_ACCEPTED_IN_FIVE_MIN};

  public TimedSourceCounter(String name) {
    super(name, ATTRIBUTES);
  }

  public Map<String, Map<String, Long>> getEventAcceptedCountInFiveMinMap() {
    return eventAcceptedCountInFiveMinMap;
  }

  public void addToEventAcceptedCountInFiveMinMap(List<Event> events) {
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
          fiveMin = TimeUtils.convertTimestampStrToFiveMinStr(timestampStr);
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

    synchronized (eventAcceptedCountInFiveMinMap) {
      for (Map.Entry<String, Long> entry : counters.entrySet()) {
        String[] arr = entry.getKey().split("\t");
        String category = arr[0];
        String fiveMin = arr[1];
        long newNum = entry.getValue();

        if (!eventAcceptedCountInFiveMinMap.containsKey(category)) {
          eventAcceptedCountInFiveMinMap.put(category, new FiveMinLinkedHashMap<String, Long>());
        }
        if (!eventAcceptedCountInFiveMinMap.get(category).containsKey(fiveMin)) {
          eventAcceptedCountInFiveMinMap.get(category).put(fiveMin, 0L);
        }

        Long oldNum = eventAcceptedCountInFiveMinMap.get(category).get(fiveMin);
        eventAcceptedCountInFiveMinMap.get(category).put(fiveMin, oldNum + newNum);
      }
    }
  }

  @Override
  public String getEventAcceptedCountInFiveMinJson() {
    return gson.toJson(eventAcceptedCountInFiveMinMap, EVENT_ACCEPTED_COUNT_IN_FIVE_MIN_MAP_TYPE);
  }

  private class FiveMinLinkedHashMap<String, Long> extends LinkedHashMap<String, Long> {
    private static final int maxSize = 500;

    public FiveMinLinkedHashMap() {
      super(16, 0.75f, false);
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
      if (size() > maxSize) {
        return true;
      } else {
        return false;
      }
    }
  }
}