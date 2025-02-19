package org.apache.hadoop.hbase.util;
import org.apache.hadoop.hbase.trace.DryRunTraceUtil;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@InterfaceAudience.Private
public class ConditionVariableWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(ConditionVariableWrapper.class);
  public boolean invokedByDryRun = false;

  public Condition condition;

  public ReentrantLock lock;
  public ConditionVariableWrapper(Condition condition, ReentrantLock lock) {
    this.condition = condition;
    this.lock = lock;
  }

  public void realAwait() throws InterruptedException {
    this.condition.await();
  }

  public void await() throws InterruptedException {
    LOG.info("ConditionVariableWrapper.await()");
    if(DryRunTraceUtil.isShadow()){
      LOG.info("ConditionVariableWrapper.await() isShadow");
      DryRunTraceUtil.clearBaggage();
      DryRunTraceUtil.createDryRunBaggage();
      this.lock.lock();
      return;
    }
    this.condition.await();
    if(invokedByDryRun && TraceUtil.forkCount == 0){
      LOG.info("ConditionVariableWrapper createShadowThread invoked by DryRun");
      DryRunUtil.createShadowThread(this, lock);
    }

  }

  public boolean await(long time, TimeUnit unit) throws InterruptedException{
    LOG.info("ConditionVariableWrapper.await(long time, TimeUnit unit)");
    return this.condition.await(time, unit);
  }

  public void signal() {
    LOG.info("ConditionVariableWrapper.signal()");
    if(DryRunTraceUtil.isDryRun()){
      invokedByDryRun = true;
    }
    this.condition.signal();
  }
}

