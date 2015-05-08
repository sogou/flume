package org.apache.flume.instrumentation.sogou;

import org.apache.flume.instrumentation.SinkCounterMBean;

/**
 * Created by Tao Li on 4/29/15.
 */
public interface TimedSinkCounterMBean extends SinkCounterMBean {
  String getEventDrainSuccessCountInFiveMinJson();
  String getCategoryEventDrainSuccessCountInFiveMinJson();
}
