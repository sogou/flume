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
  private Map<String, Map<String, TimedUtils.TimestampCount>> eventAcceptedCountInFiveMinMap =
          new HashMap<String, Map<String, TimedUtils.TimestampCount>>();

  private static final String COUNTER_EVENTS_ACCEPTED_IN_FIVE_MIN =
          "src.events.accepted.5min";

  private static final String[] ATTRIBUTES =
          {COUNTER_EVENTS_ACCEPTED_IN_FIVE_MIN};

  public TimedSourceCounter(String name) {
    super(name, ATTRIBUTES);
  }

  public void addToEventAcceptedCountInFiveMinMap(List<Event> events) {
    TimedUtils.updateFiveMinMap(events, eventAcceptedCountInFiveMinMap);
  }

  @Override
  public String getEventAcceptedCountInFiveMinJson() {
    return TimedUtils.convertFiveMinMapToJson(eventAcceptedCountInFiveMinMap);
  }
}