package org.apache.flume.instrumentation.sogou;

import org.apache.flume.instrumentation.kafka.KafkaChannelCounterMBean;

/**
 * Created by Tao Li on 5/8/15.
 */
public interface TimedKafkaChannelCounterMBean extends KafkaChannelCounterMBean {
  String getEventPutSuccessCountInFiveMinJson();

  String getEventTakeSuccessCountInFiveMinJson();

  String getRollbackCountInFiveMinJson();
}