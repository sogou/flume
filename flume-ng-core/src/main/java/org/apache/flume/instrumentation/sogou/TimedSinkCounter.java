package org.apache.flume.instrumentation.sogou;

import org.apache.flume.Event;
import org.apache.flume.instrumentation.SinkCounter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Tao Li on 4/29/15.
 */
public class TimedSinkCounter extends SinkCounter implements TimedSinkCounterMBean {
  private Map<String, TimedUtils.TimestampCount> eventDrainSuccessCountInFiveMinMap =
      new TimedUtils.FiveMinLinkedHashMap();
  private Map<String, Map<String, TimedUtils.TimestampCount>> categoryEventDrainSuccessCountInFiveMinMap =
      new HashMap<String, Map<String, TimedUtils.TimestampCount>>();

  private static final String COUNTER_EVENT_DRAIN_SUCCESS_IN_FIVE_MIN =
      "sink.event.drain.sucess.5min";
  private static final String COUNTER_CATEGORY_EVENT_DRAIN_SUCCESS_IN_FIVE_MIN =
      "sink.category.event.drain.sucess.5min";

  private static final String[] ATTRIBUTES =
      {
          COUNTER_EVENT_DRAIN_SUCCESS_IN_FIVE_MIN, COUNTER_CATEGORY_EVENT_DRAIN_SUCCESS_IN_FIVE_MIN
      };

  public TimedSinkCounter(String name) {
    super(name, ATTRIBUTES);
  }

  public void addToEventDrainSuccessCountInFiveMinMap(long delta) {
    TimedUtils.updateFiveMinMap(delta, eventDrainSuccessCountInFiveMinMap);
  }

  @Override
  public String getEventDrainSuccessCountInFiveMinJson() {
    return TimedUtils.convertFiveMinMapToJson(eventDrainSuccessCountInFiveMinMap);
  }

  public Map<String, TimedUtils.TimestampCount> getEventDrainSuccessCountInFiveMinMap() {
    return eventDrainSuccessCountInFiveMinMap;
  }

  public void addToCategoryEventDrainSuccessCountInFiveMinMap(List<Event> events, String categoryKey) {
    TimedUtils.updateCategoryFiveMinMap(events, categoryEventDrainSuccessCountInFiveMinMap, categoryKey);
  }

  public void addToCategoryEventDrainSuccessCountInFiveMinMap(List<Event> events) {
    TimedUtils.updateCategoryFiveMinMap(events, categoryEventDrainSuccessCountInFiveMinMap);
  }

  @Override
  public String getCategoryEventDrainSuccessCountInFiveMinJson() {
    return TimedUtils.convertCategoryFiveMinMapToJson(categoryEventDrainSuccessCountInFiveMinMap);
  }
}