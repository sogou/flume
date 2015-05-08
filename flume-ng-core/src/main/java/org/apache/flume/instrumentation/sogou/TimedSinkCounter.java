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
  private Map<String, Map<String, TimedUtils.TimestampCount>> eventDrainSuccessCountInFiveMinMap =
          new HashMap<String, Map<String, TimedUtils.TimestampCount>>();

  private static final String COUNTER_EVENT_DRAIN_SUCCESS_IN_FIVE_MIN =
          "sink.event.drain.sucess.5min";

  private static final String[] ATTRIBUTES =
          {COUNTER_EVENT_DRAIN_SUCCESS_IN_FIVE_MIN};

  public TimedSinkCounter(String name) {
    super(name, ATTRIBUTES);
  }

  public void addToEventDrainSuccessCountInFiveMinMap(List<Event> events) {
    TimedUtils.updateCategoryFiveMinMap(events, eventDrainSuccessCountInFiveMinMap);
  }

  @Override
  public String getEventDrainSuccessCountInFiveMinJson() {
    return TimedUtils.convertCategoryFiveMinMapToJson(eventDrainSuccessCountInFiveMinMap);
  }
}