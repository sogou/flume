package org.apache.flume.instrumentation.sogou;

import org.apache.flume.instrumentation.kafka.KafkaChannelCounter;

import java.util.Map;

/**
 * Created by Tao Li on 5/8/15.
 */
public class TimedKafkaChannelCounter extends KafkaChannelCounter implements TimedKafkaChannelCounterMBean {
  private Map<String, TimedUtils.TimestampCount> eventPutSuccessCountInFiveMinMap =
      new TimedUtils.FiveMinLinkedHashMap<String, TimedUtils.TimestampCount>();
  private Map<String, TimedUtils.TimestampCount> eventTakeSuccessCountInFiveMinMap =
      new TimedUtils.FiveMinLinkedHashMap<String, TimedUtils.TimestampCount>();
  private Map<String, TimedUtils.TimestampCount> rollbackCountInFiveMinMap =
      new TimedUtils.FiveMinLinkedHashMap<String, TimedUtils.TimestampCount>();

  private static final String COUNTER_EVENT_PUT_SUCCESS_IN_FIVE_MIN =
      "channel.event.put.success.5min";
  private static final String COUNTER_EVENT_TAKE_SUCCESS_IN_FIVE_MIN =
      "channel.event.take.success.5min";
  private static final String COUNT_ROLLBACK_IN_FIVE_MIN =
      "channel.rollback.count.5min";

  private static final String[] ATTRIBUTES =
      {
          COUNTER_EVENT_PUT_SUCCESS_IN_FIVE_MIN, COUNTER_EVENT_TAKE_SUCCESS_IN_FIVE_MIN,
          COUNT_ROLLBACK_IN_FIVE_MIN
      };

  public TimedKafkaChannelCounter(String name) {
    super(name, ATTRIBUTES);
  }

  public void addToEventPutSuccessCountInFiveMinMap(long delta) {
    TimedUtils.updateFiveMinMap(delta, eventPutSuccessCountInFiveMinMap);
  }

  @Override
  public String getEventPutSuccessCountInFiveMinJson() {
    return TimedUtils.convertFiveMinMapToJson(eventPutSuccessCountInFiveMinMap);
  }

  public void addToEventTakeSuccessCountInFiveMinMap(long delta) {
    TimedUtils.updateFiveMinMap(delta, eventTakeSuccessCountInFiveMinMap);
  }

  @Override
  public String getEventTakeSuccessCountInFiveMinJson() {
    return TimedUtils.convertFiveMinMapToJson(eventTakeSuccessCountInFiveMinMap);
  }

  public void addToRollbackCountInFiveMinMap(long delta) {
    TimedUtils.updateFiveMinMap(delta, rollbackCountInFiveMinMap);
  }

  @Override
  public String getRollbackCountInFiveMinJson() {
    return TimedUtils.convertFiveMinMapToJson(rollbackCountInFiveMinMap);
  }
}
