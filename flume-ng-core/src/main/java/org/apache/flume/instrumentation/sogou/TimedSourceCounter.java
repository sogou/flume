package org.apache.flume.instrumentation.sogou;

import org.apache.flume.Event;
import org.apache.flume.instrumentation.SourceCounter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Tao Li on 4/29/15.
 */
public class TimedSourceCounter extends SourceCounter implements TimedSourceCounterMBean {
  private Map<String, TimedUtils.TimestampCount> eventReceivedCountInFiveMinMap =
      new TimedUtils.FiveMinLinkedHashMap<String, TimedUtils.TimestampCount>();
  private Map<String, TimedUtils.TimestampCount> eventAcceptedCountInFiveMinMap =
      new TimedUtils.FiveMinLinkedHashMap<String, TimedUtils.TimestampCount>();
  private Map<String, Map<String, TimedUtils.TimestampCount>> categoryEventAcceptedCountInFiveMinMap =
      new HashMap<String, Map<String, TimedUtils.TimestampCount>>();

  private static final String COUNTER_EVENTS_RECEIVED_IN_FIVE_MIN =
      "src.events.received.5min";
  private static final String COUNTER_EVENTS_ACCEPTED_IN_FIVE_MIN =
      "src.events.accepted.5min";
  private static final String COUNTER_CATEGORY_EVENTS_ACCEPTED_IN_FIVE_MIN =
      "src.category.events.accepted.5min";

  private static final String[] ATTRIBUTES =
      {
          COUNTER_EVENTS_RECEIVED_IN_FIVE_MIN, COUNTER_EVENTS_ACCEPTED_IN_FIVE_MIN,
          COUNTER_CATEGORY_EVENTS_ACCEPTED_IN_FIVE_MIN
      };

  public TimedSourceCounter(String name) {
    super(name, ATTRIBUTES);
  }

  public void addToEventReceivedCountInFiveMinMap(long delta) {
    TimedUtils.updateFiveMinMap(delta, eventReceivedCountInFiveMinMap);
  }

  @Override
  public String getEventReceivedCountInFiveMinJson() {
    return TimedUtils.convertFiveMinMapToJson(eventReceivedCountInFiveMinMap);
  }

  public void addToEventAcceptedCountInFiveMinMap(long delta) {
    TimedUtils.updateFiveMinMap(delta, eventAcceptedCountInFiveMinMap);
  }

  @Override
  public String getEventAcceptedCountInFiveMinJson() {
    return TimedUtils.convertFiveMinMapToJson(eventAcceptedCountInFiveMinMap);
  }

  public void addToCategoryEventAcceptedCountInFiveMinMap(List<Event> events) {
    TimedUtils.updateCategoryFiveMinMap(events, categoryEventAcceptedCountInFiveMinMap);
  }

  @Override
  public String getCategoryEventAcceptedCountInFiveMinJson() {
    return TimedUtils.convertCategoryFiveMinMapToJson(categoryEventAcceptedCountInFiveMinMap);
  }
}