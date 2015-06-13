package org.apache.flume.channel.sogou;

import org.apache.flume.Event;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;

/**
 * Created by Tao Li on 6/13/15.
 */
public class NullChannel extends BasicChannelSemantics {
  private class NullTransaction extends BasicTransactionSemantics {

    @Override
    protected void doPut(Event event) throws InterruptedException {
    }

    @Override
    protected Event doTake() throws InterruptedException {
      return null;
    }

    @Override
    protected void doCommit() throws InterruptedException {
    }

    @Override
    protected void doRollback() throws InterruptedException {
    }
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
    return new NullTransaction();
  }
}
