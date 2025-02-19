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
package org.apache.hadoop.hbase.trace;

import com.rits.cloning.Cloner;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.Scope;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.Version;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public final class TraceUtil {

  public static int forkCount = 0;

  public static Object callerClass = null;

  public static Map<String, String> executingUnits = new HashMap<>();

  public static Set<Long> originalThreads = new HashSet<>();
  private TraceUtil() {

  }

  public static Tracer getGlobalTracer() {
    return GlobalOpenTelemetry.getTracer("org.apache.hbase", Version.version);
  }

  /**
   * Create a {@link SpanKind#INTERNAL} span.
   */
  public static Span createSpan(String name) {
    return createSpan(name, SpanKind.INTERNAL);
  }

  /**
   * Create a span with the given {@code kind}. Notice that, OpenTelemetry only expects one
   * {@link SpanKind#CLIENT} span and one {@link SpanKind#SERVER} span for a traced request, so use
   * this with caution when you want to create spans with kind other than {@link SpanKind#INTERNAL}.
   */
  private static Span createSpan(String name, SpanKind kind) {
    return getGlobalTracer().spanBuilder(name).setSpanKind(kind).startSpan();
  }

  /**
   * Create a span which parent is from remote, i.e, passed through rpc.
   * </p>
   * We will set the kind of the returned span to {@link SpanKind#SERVER}, as this should be the top
   * most span at server side.
   */
  public static Span createRemoteSpan(String name, Context ctx) {
    return getGlobalTracer().spanBuilder(name).setParent(ctx).setSpanKind(SpanKind.SERVER)
      .startSpan();
  }

  /**
   * Create a span with {@link SpanKind#CLIENT}.
   */
  public static Span createClientSpan(String name) {
    return createSpan(name, SpanKind.CLIENT);
  }

  /**
   * Trace an asynchronous operation for a table.
   */
  public static <T> CompletableFuture<T> tracedFuture(Supplier<CompletableFuture<T>> action,
    Supplier<Span> spanSupplier) {
    Span span = spanSupplier.get();
    try (Scope ignored = span.makeCurrent()) {
      CompletableFuture<T> future = action.get();
      endSpan(future, span);
      return future;
    }
  }

  /**
   * Trace an asynchronous operation.
   */
  public static <T> CompletableFuture<T> tracedFuture(Supplier<CompletableFuture<T>> action,
    String spanName) {
    Span span = createSpan(spanName);
    try (Scope ignored = span.makeCurrent()) {
      CompletableFuture<T> future = action.get();
      endSpan(future, span);
      return future;
    }
  }

  /**
   * Trace an asynchronous operation, and finish the create {@link Span} when all the given
   * {@code futures} are completed.
   */
  public static <T> List<CompletableFuture<T>>
  tracedFutures(Supplier<List<CompletableFuture<T>>> action, Supplier<Span> spanSupplier) {
    Span span = spanSupplier.get();
    try (Scope ignored = span.makeCurrent()) {
      List<CompletableFuture<T>> futures = action.get();
      endSpan(CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])), span);
      return futures;
    }
  }

  public static void setError(Span span, Throwable error) {
    span.recordException(error);
    span.setStatus(StatusCode.ERROR);
  }

  /**
   * Finish the {@code span} when the given {@code future} is completed.
   */
  private static void endSpan(CompletableFuture<?> future, Span span) {
    FutureUtils.addListener(future, (resp, error) -> {
      if (error != null) {
        setError(span, error);
      } else {
        span.setStatus(StatusCode.OK);
      }
      span.end();
    });
  }

  /**
   * Wrap the provided {@code runnable} in a {@link Runnable} that is traced.
   */
  public static Runnable tracedRunnable(final Runnable runnable, final String spanName) {
    return tracedRunnable(runnable, () -> createSpan(spanName));
  }

  /**
   * Wrap the provided {@code runnable} in a {@link Runnable} that is traced.
   */
  public static Runnable tracedRunnable(final Runnable runnable,
    final Supplier<Span> spanSupplier) {
    // N.B. This method name follows the convention of this class, i.e., tracedFuture, rather than
    // the convention of the OpenTelemetry classes, i.e., Context#wrap.
    return () -> {
      final Span span = spanSupplier.get();
      try (final Scope ignored = span.makeCurrent()) {
        runnable.run();
        span.setStatus(StatusCode.OK);
      } finally {
        span.end();
      }
    };
  }

  /**
   * A {@link Runnable} that may also throw.
   * @param <T> the type of {@link Throwable} that can be produced.
   */
  @FunctionalInterface
  public interface ThrowingRunnable<T extends Throwable> {
    void run() throws T;
  }

  /**
   * Trace the execution of {@code runnable}.
   */
  public static <T extends Throwable> void trace(final ThrowingRunnable<T> runnable,
    final String spanName) throws T {
    trace(runnable, () -> createSpan(spanName));
  }

  /**
   * Trace the execution of {@code runnable}.
   */
  public static <T extends Throwable> void trace(final ThrowingRunnable<T> runnable,
    final Supplier<Span> spanSupplier) throws T {
    Span span = spanSupplier.get();
    try (Scope ignored = span.makeCurrent()) {
      runnable.run();
      span.setStatus(StatusCode.OK);
    } catch (Throwable e) {
      setError(span, e);
      throw e;
    } finally {
      span.end();
    }
  }

  /**
   * A {@link Callable} that may also throw.
   * @param <R> the result type of method call.
   * @param <T> the type of {@link Throwable} that can be produced.
   */
  @FunctionalInterface
  public interface ThrowingCallable<R, T extends Throwable> {
    R call() throws T;
  }

  public static <R, T extends Throwable> R trace(final ThrowingCallable<R, T> callable,
    final String spanName) throws T {
    return trace(callable, () -> createSpan(spanName));
  }

  public static <R, T extends Throwable> R trace(final ThrowingCallable<R, T> callable,
    final Supplier<Span> spanSupplier) throws T {
    Span span = spanSupplier.get();
    try (Scope ignored = span.makeCurrent()) {
      final R ret = callable.call();
      span.setStatus(StatusCode.OK);
      return ret;
    } catch (Throwable e) {
      setError(span, e);
      throw e;
    } finally {
      span.end();
    }
  }

  public static boolean debug = false;
  public static final String DRY_RUN_KEY = "is_dry_run";
  public static final String FAST_FORWARD_KEY = "is_fast_forward";

  public static final String IS_SHADOW_THREAD_KEY = "is_shadow_thread";

  public static final String SHOULD_RELEASE_LOCK_KEY = "should_release_lock";
  public static ContextKey<Boolean> IS_DRY_RUN = ContextKey.named("is_dry_run");

  public static boolean isDryRun() {
    if(debug){
      return false;
    }else{
      return Baggage.current().getEntryValue(DRY_RUN_KEY) != null && Boolean.parseBoolean(Baggage.current().getEntryValue(DRY_RUN_KEY));
    }
    //return false;
  }

  public static boolean isShadow() {
    if(debug){
      return false;
    }else{
      return Baggage.current().getEntryValue(IS_SHADOW_THREAD_KEY) != null && Boolean.parseBoolean(Baggage.current().getEntryValue(IS_SHADOW_THREAD_KEY));
    }
    //return false;
  }

  public static boolean isFastForward() {
    //print thread information
    boolean res = Baggage.current().getEntryValue(FAST_FORWARD_KEY) != null && Boolean.parseBoolean(Baggage.current().getEntryValue(FAST_FORWARD_KEY));
    System.out.println("Thread: " + Thread.currentThread().getName() + "result is: " + res)  ;
    if(debug){
      return false;
    }else{
      return res;
    }
  }

  public static Scope getDryRunTraceScope(boolean needsDryRunTrace) {
    if (!needsDryRunTrace) {
      return Baggage.empty().makeCurrent();
    }
    Baggage dryRunBaggage = Baggage.current().toBuilder()
      .put(DRY_RUN_KEY, "true")
      .build();
    return dryRunBaggage.makeCurrent();
  }

  public static Baggage createFastForwardBaggage(boolean flag) {
    if(!flag){
      return null;
    }
    Baggage fastForwardBaggage = Baggage.current().toBuilder().put(FAST_FORWARD_KEY, "true").build();
    fastForwardBaggage.makeCurrent();
    Context.current().with(fastForwardBaggage).makeCurrent();
    return fastForwardBaggage;
  }

  public static Baggage createShadowBaggage() {
    Baggage shadowBaggage = Baggage.current().toBuilder().put(IS_SHADOW_THREAD_KEY, "true").build();
    shadowBaggage.makeCurrent();
    Context.current().with(shadowBaggage).makeCurrent();
    return shadowBaggage;
  }

  public static Baggage createFastForwardBaggage() {
    Baggage fastForwardBaggage = Baggage.current().toBuilder().put(FAST_FORWARD_KEY, "true").build();
    fastForwardBaggage.makeCurrent();
    Context.current().with(fastForwardBaggage).makeCurrent();
    return fastForwardBaggage;
  }

  public static Baggage createDryRunBaggage() {
    Baggage dryRunBaggage = Baggage.current().toBuilder().put(DRY_RUN_KEY, "true").build();
    dryRunBaggage.makeCurrent();
    Context.current().with(dryRunBaggage).makeCurrent();
    return dryRunBaggage;
  }

  public static void removeDryRunBaggage() {
    Baggage emptyBaggage = Baggage.empty();
    emptyBaggage.makeCurrent();
    Context.current().with(emptyBaggage).makeCurrent();
  }

  public static void clearBaggage() {
    System.out.println("Clearing baggage");
    Baggage emptyBaggage = Baggage.empty();
    emptyBaggage.makeCurrent();
    Context.current().with(emptyBaggage).makeCurrent();
  }

  private static final class ExecutionUnit {
    final String methodSignature;
    final String unitId;

    ExecutionUnit(String methodSignature, String unitId) {
      this.methodSignature = methodSignature;
      this.unitId = unitId;
    }
  }

  private static final ThreadLocal<Integer> index = ThreadLocal.withInitial(() -> 0);
  private static final ThreadLocal<ArrayList<ExecutionUnit>> executionArray =
    ThreadLocal.withInitial(ArrayList::new);

  public static void recordExecutingUnit(String methodSig, String unitId) {
    ArrayList<ExecutionUnit> array = executionArray.get();

    array.add(new ExecutionUnit(methodSig, unitId));

  }

  public static int getExecutingUnit() {
    int index = TraceUtil.index.get();

    ArrayList<ExecutionUnit> array = executionArray.get();

    assert index >= 0 && index < array.size();
    ExecutionUnit unit = array.get(index);
    TraceUtil.index.set(index + 1);

    return Integer.parseInt(unit.unitId);
  }


}
