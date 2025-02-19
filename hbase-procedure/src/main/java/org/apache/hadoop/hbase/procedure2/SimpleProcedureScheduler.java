/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.procedure2;

import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Simple scheduler for procedures
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SimpleProcedureScheduler extends AbstractProcedureScheduler {
  private final ProcedureDeque runnables = new ProcedureDeque();

  private final ProcedureDeque runnables$dryrun = new ProcedureDeque();

  @Override
  protected void enqueue(final Procedure procedure, final boolean addFront) {
    if(TraceUtil.isDryRun()){
      enqueue$instrumentation(procedure, addFront);
      return;
    }
    System.out.println("enqueue.procedure = " + procedure);
    if (addFront) {
      runnables.addFirst(procedure);
    } else {
      runnables.addLast(procedure);
    }
  }

  protected void enqueue$instrumentation(final Procedure procedure, final boolean addFront) {
    if (addFront) {
      runnables$dryrun.addFirst(procedure);
      runnables.isTainted = true;
    } else {
      runnables$dryrun.addLast(procedure);
      runnables.isTainted = true;
    }
  }

  @Override
  protected Procedure dequeue() {
    if(TraceUtil.isDryRun()){
      return dequeue$instrumentation();
    }
    return runnables.poll();
  }

  protected Procedure dequeue$instrumentation() {
    return runnables$dryrun.poll();
  }

  @Override
  public void clear() {
    schedLock();
    try {
      runnables.clear();
    } finally {
      schedUnlock();
    }
  }

  @Override
  public void yield(final Procedure proc) {
    addBack(proc);
  }

  @Override
  public boolean queueHasRunnables() {
    System.out.println("enter queueHasRunnables");
    int res = runnables.size();
    System.out.println("queueHasRunnables.size() = " + res);
    return res>0;
  }

  public boolean queueHasRunnables$shadow(){
    System.out.println("enter queueHasRunnables$shadow");
    int res = runnables$dryrun.size();
    System.out.println("queueHasRunnables.size() = " + res);
    return res>0;
  }

  @Override
  public int queueSize() {
    return runnables.size();
  }

  @Override
  public void completionCleanup(Procedure proc) {
  }

  @Override
  public List<LockedResource> getLocks() {
    return Collections.emptyList();
  }

  @Override
  public LockedResource getLockResource(LockedResourceType resourceType, String resourceName) {
    return null;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).appendSuper(super.toString())
      .append("runnables", runnables).build();
  }
}
