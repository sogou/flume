package org.apache.flume.channel.sogou;

import org.apache.flume.Event;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Tao Li on 6/13/15.
 */
public class NullChannel extends BasicChannelSemantics {
  private static Logger LOGGER = LoggerFactory.getLogger(NullChannel.class);


  private class NullTransaction extends BasicTransactionSemantics {

    @Override
    protected void doPut(Event event) throws InterruptedException {
      LOGGER.debug("Call doPut: " + event);
    }

    @Override
    protected Event doTake() throws InterruptedException {
      LOGGER.debug("Call doTake");
      return null;
    }

    @Override
    protected void doCommit() throws InterruptedException {
      LOGGER.debug("Call doCommit");
    }

    @Override
    protected void doRollback() throws InterruptedException {
      LOGGER.debug("Call doRollback");
    }
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
    return new NullTransaction();
  }
}
