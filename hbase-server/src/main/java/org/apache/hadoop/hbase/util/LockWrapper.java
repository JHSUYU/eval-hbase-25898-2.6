package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.trace.DryRunTraceUtil;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantLock;
@InterfaceAudience.Private
public class LockWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(LockWrapper.class);

  public ReentrantLock lock;
  public boolean modifiedByDryRun = false;

  public LockWrapper(ReentrantLock lock) {
    this.lock = lock;
  }

  public void realLock() {
    this.lock.lock();
  }

  public void realUnlock() {
    this.lock.unlock();
  }

  public void lock() {
    if(DryRunTraceUtil.isShadow()){
      DryRunTraceUtil.clearBaggage();
      DryRunTraceUtil.createDryRunBaggage();
    }

    this.lock.lock();

    if(modifiedByDryRun && !TraceUtil.isDryRun() && TraceUtil.forkCount == 0){
      LOG.info("LockWrapper createShadowThread");
      DryRunUtil.createShadowThread(this);
    }
  }

  public void unlock() {
    LOG.info("Release DryRun lock");
    if(DryRunTraceUtil.isDryRun()){
      LOG.info("Release DryRun Lock");
      this.modifiedByDryRun = true;
    }
    this.lock.unlock();
  }
}
