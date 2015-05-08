package org.apache.flume.instrumentation.sogou;

import org.apache.flume.instrumentation.SourceCounterMBean;

/**
 * Created by Tao Li on 4/29/15.
 */
public interface TimedSourceCounterMBean extends SourceCounterMBean {
  String getEventReceivedCountInFiveMinJson();
  String getEventAcceptedCountInFiveMinJson();
  String getCategoryEventAcceptedCountInFiveMinJson();
}
