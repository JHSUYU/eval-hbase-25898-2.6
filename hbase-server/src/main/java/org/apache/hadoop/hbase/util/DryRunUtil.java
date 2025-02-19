package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.yetus.audience.InterfaceAudience;
import java.util.concurrent.locks.ReentrantLock;

@InterfaceAudience.Private
public class DryRunUtil {

  public static AssignmentManager target;

  public static void setTarget(AssignmentManager target) {
    DryRunUtil.target = target;
  }

  public static void createShadowThread(){
     System.out.println("createShadowThread, target is null? " + (target == null));

     if(target != null){
       target.createShadowThread();
     }
  }

  public static void createShadowThread(LockWrapper lockWrapper){
    System.out.println("createShadowThread, target is null lockWrapper Version? " + (target == null));
    lockWrapper.realUnlock();
    if(target != null){
      target.createShadowThread();
    }
    lockWrapper.realLock();
  }

  public static void createShadowThread(ConditionVariableWrapper conditionVariableWrapper, ReentrantLock lock)
    throws InterruptedException {
    System.out.println("createShadowThread, target is null conditionVariableWrapper version? " + (target == null));
    lock.unlock();
    if(target != null){
      target.createShadowThread();
    }
    conditionVariableWrapper.realAwait();
  }

}
