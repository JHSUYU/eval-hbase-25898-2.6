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
package org.apache.hadoop.hbase.util;

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
import org.apache.hadoop.hbase.trace.WrapContext;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public final class TraceUtil {

  public static int forkCount = 0;

  public static Object callerClass = null;

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

  public static final Logger dryRunLogger = LoggerFactory.getLogger(TraceUtil.class);

  public static Cloner cloner = new Cloner();
  private static Map<String, HashMap<String, WrapContext>> stateMap = new HashMap();

  private static Map<String, HashMap<String, WrapContext>> fieldMap = new HashMap();




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
    System.out.println("Thread: " + Thread.currentThread().getName());
    if(debug){
      return false;
    }else{
      return Baggage.current().getEntryValue(FAST_FORWARD_KEY) != null && Boolean.parseBoolean(Baggage.current().getEntryValue(FAST_FORWARD_KEY));
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

  //print all the information in the stateMap
  public static void printStateMap(){
    System.out.println("Printing state map");
    for(Map.Entry<String, HashMap<String, WrapContext>> entry: stateMap.entrySet()){
      String methodName = entry.getKey();
      Map<String, WrapContext> state = entry.getValue();
      System.out.println("Method: " + methodName);
      for(Map.Entry<String, WrapContext> entry2: state.entrySet()){
        String varName = entry2.getKey();
        WrapContext value = entry2.getValue();
        System.out.println("Variable: " + varName + " Value: " + value.value);
      }
    }
  }

  public static void recordState(String methodSig, HashMap<String, WrapContext> state){
    System.out.println("Recording state for method: " + methodSig);
    HashMap<String, WrapContext> varMap = new HashMap<>();

    for(Map.Entry<String, WrapContext> entry: state.entrySet()){
      String name = entry.getKey();
      //Object value = cloner.deepClone(entry.getValue());
      WrapContext value = entry.getValue();
      //      if(value.value != null){
      //        System.out.println("Value.value classname is: " + value.value.getClass().getName());
      ////        if(shouldBeDeepCloned(value.value.getClass().getName())){
      ////          value.value = cloner.deepClone(value.value);
      ////        }
      //      }
      varMap.put(name, value);
    }

    String tmp = methodSig;
    if(methodSig.contains("access$")){
      tmp = methodSig.replaceAll("[0-9]", "");
    }

    stateMap.putIfAbsent(tmp, varMap);

  }

  public static void recordFieldState(String methodSig, HashMap<String, WrapContext> state){
    System.out.println("Recording field for method: " + methodSig);
    HashMap<String, WrapContext> varMap = new HashMap<>();

    for(Map.Entry<String, WrapContext> entry: state.entrySet()){
      String name = entry.getKey();
      WrapContext value = entry.getValue();
      //System.out.println("Value.value classname is: " + value.value.getClass().getName());
      //      if(shouldBeDeepCloned(value.value.getClass().getName())){
      //        value.value = cloner.deepClone(value.value);
      //      }
      varMap.put(name, value);
    }

    String tmp = methodSig;
    if(methodSig.contains("access$")){
      tmp = methodSig.replaceAll("[0-9]", "");
    }

    fieldMap.putIfAbsent(tmp, varMap);

  }

  public static boolean shouldBeDeepCloned(String className){
    if(className.contains("java.lang.ref.WeakReference") || className.contains("org.apache.hadoop.hbase.io.hfile.LruBlockCache")
      || className.contains("java.util.HashMap") || className.contains("java.util.concurrent.locks.ReentrantLock")
      || className.contains("org.apache.logging.slf4j.Log4jLogger")){
      return false;
    }
    return true;
  }

  public static HashMap<String, WrapContext> getState(String methodSig){
    System.out.println("Getting state for method: " + methodSig);

    String methodNameTmp = methodSig.replace("$shadow","");
    methodNameTmp = methodNameTmp.replace("Shadow","");

    if(methodNameTmp.contains("access$")){
      methodNameTmp = methodSig.replaceAll("[0-9]", "");
    }

    HashMap<String, WrapContext> state = stateMap.get(methodNameTmp);
    //print key, value in state
    System.out.println("Getting state for method: " + methodNameTmp);
    for(Map.Entry<String, WrapContext> entry: state.entrySet()){
      String varName = entry.getKey();
      WrapContext value = entry.getValue();
      System.out.println("Variable: " + varName + " Value: " + value.value);
    }
    return state;
  }

  public static HashMap<String, WrapContext> getFieldState(String methodSig){
    System.out.println("Getting Fiedl state for method: " + methodSig);

    String methodNameTmp = methodSig.replace("$shadow","");
    methodNameTmp = methodNameTmp.replace("Shadow","");

    if(methodNameTmp.contains("access$")){
      methodNameTmp = methodSig.replaceAll("[0-9]", "");
    }

    HashMap<String, WrapContext> state = fieldMap.get(methodNameTmp);
    //print key, value in state
    System.out.println("Getting field for method: " + methodNameTmp);
    for(Map.Entry<String, WrapContext> entry: state.entrySet()){
      String varName = entry.getKey();
      WrapContext value = entry.getValue();
      System.out.println("Getting fieldState Variable: " + varName + " Value: " + value.value);
    }
    return state;
  }

  //  public static void createShadowThread(){
  //    Baggage fastForwardBaggage = Baggage.current().toBuilder()
  //      .put(FAST_FORWARD_KEY, "true")
  //      .build();
  //    Context fastForwardContext = Context.current().with(fastForwardBaggage);
  //    fastForwardContext.makeCurrent();
  //
  //    ExecutorService baseExecutor = new ThreadPoolExecutor(
  //      1,  // corePoolSize
  //      1,  // maximumPoolSize
  //      0L, // keepAliveTime
  //      TimeUnit.MILLISECONDS,  // TimeUnit for keepAliveTime
  //      new LinkedBlockingQueue<>()  // workQueue
  //    );
  //
  //    ShadowMicroFork shadowThread = new ShadowMicroFork(1, true, 1, true);
  //    shadowThread.lock = new ReentrantLock();
  //    Future<?> future = baseExecutor.submit(shadowThread);
  //
  //    try {
  //      future.get();
  //      System.out.println("field3 value after execution: " + ((ShadowMicroFork)shadowThread).field3);
  //    } catch(InterruptedException | ExecutionException e) {
  //      e.printStackTrace();
  //    } finally {
  //      baseExecutor.shutdown();
  //    }
  //
  //  }

  public static void clearStates() {
    stateMap.clear();
  }

  public static void recordMicroForkCaller (Object classCaller){
    callerClass = classCaller;
  }


  //  public static boolean shouldBeContextWrap(Runnable runnable, Executor executor) {
  //    if (executor instanceof AbstractLocalAwareExecutorService){
  //      return false;
  //    }
  //    if (runnable instanceof FutureTask){
  //      return true;
  //    }
  //    return false;
  //  }
}
