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
package org.apache.hadoop.hbase.tool;

import static org.apache.hadoop.hbase.regionserver.TestRegionServerNoMaster.closeRegion;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.ArgumentMatcher;

@Category({ LargeTests.class })
public class TestCanaryTool {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCanaryTool.class);

  private HBaseTestingUtility testingUtility;
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] COLUMN = Bytes.toBytes("col");

  @Rule
  public TestName name = new TestName();

  private org.apache.logging.log4j.core.Appender mockAppender;

  @Before
  public void setUp() throws Exception {
    testingUtility = new HBaseTestingUtility();
    testingUtility.startMiniCluster();
    mockAppender = mock(org.apache.logging.log4j.core.Appender.class);
    when(mockAppender.getName()).thenReturn("mockAppender");
    when(mockAppender.isStarted()).thenReturn(true);
    ((org.apache.logging.log4j.core.Logger) org.apache.logging.log4j.LogManager
      .getLogger("org.apache.hadoop.hbase")).addAppender(mockAppender);
  }

  @After
  public void tearDown() throws Exception {
    testingUtility.shutdownMiniCluster();
    ((org.apache.logging.log4j.core.Logger) org.apache.logging.log4j.LogManager
      .getLogger("org.apache.hadoop.hbase")).removeAppender(mockAppender);
  }

  @Test
  public void testBasicZookeeperCanaryWorks() throws Exception {
    final String[] args = { "-t", "10000", "-zookeeper" };
    testZookeeperCanaryWithArgs(args);
  }

  @Test
  public void testZookeeperCanaryPermittedFailuresArgumentWorks() throws Exception {
    final String[] args =
      { "-t", "10000", "-zookeeper", "-treatFailureAsError", "-permittedZookeeperFailures", "1" };
    testZookeeperCanaryWithArgs(args);
  }

  @Test
  public void testBasicCanaryWorks() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    Table table = testingUtility.createTable(tableName, new byte[][] { FAMILY });
    // insert some test rows
    for (int i = 0; i < 1000; i++) {
      byte[] iBytes = Bytes.toBytes(i);
      Put p = new Put(iBytes);
      p.addColumn(FAMILY, COLUMN, iBytes);
      table.put(p);
    }
    ExecutorService executor = new ScheduledThreadPoolExecutor(1);
    CanaryTool.RegionStdOutSink sink = spy(new CanaryTool.RegionStdOutSink());
    CanaryTool canary = new CanaryTool(executor, sink);
    String[] args = { "-writeSniffing", "-t", "10000", tableName.getNameAsString() };
    assertEquals(0, ToolRunner.run(testingUtility.getConfiguration(), canary, args));
    assertEquals("verify no read error count", 0, canary.getReadFailures().size());
    assertEquals("verify no write error count", 0, canary.getWriteFailures().size());
    verify(sink, atLeastOnce()).publishReadTiming(isA(ServerName.class), isA(RegionInfo.class),
      isA(ColumnFamilyDescriptor.class), anyLong());
  }

  /**
   * When CanaryTool times out, it should stop scanning and shutdown quickly and gracefully. This
   * test helps to confirm that threadpools do not continue executing work after the canary
   * finishes. It also verifies sink behavior and measures correct failure counts in the sink.
   * @throws Exception if it can't create a table, communicate with minicluster, or run the canary.
   */
  @Test
  public void testCanaryStopsScanningAfterTimeout() throws Exception {
    // Prepare a table with multiple regions, and close those regions on the regionserver.
    // Do not notify HMaster or META. CanaryTool will scan and receive NotServingRegionExceptions.
    final TableName tableName = TableName.valueOf(name.getMethodName());
    // Close the unused Table reference returned by createMultiRegionTable.
    testingUtility.createMultiRegionTable(tableName, new byte[][] { FAMILY }).close();
    List<RegionInfo> regions = testingUtility.getAdmin().getRegions(tableName);
    assertTrue("verify table has multiple regions", regions.size() > 1);
    HRegionServer regionserver = testingUtility.getMiniHBaseCluster().getRegionServer(0);
    for (RegionInfo region : regions) {
      closeRegion(testingUtility, regionserver, new HRegionInfo(region));
    }

    // Run CanaryTool with 1 thread. This thread will attempt to scan the first region.
    // It will use default rpc retries and receive NotServingRegionExceptions for many seconds
    // according to HConstants.RETRY_BACKOFF. The CanaryTool timeout is set to 4 seconds, so it
    // will time out before the first region scan is complete.
    ExecutorService executor = new ScheduledThreadPoolExecutor(1);
    CanaryTool canary = new CanaryTool(executor);
    String[] args = { "-t", "4000", tableName.getNameAsString() };
    int retCode = ToolRunner.run(testingUtility.getConfiguration(), canary, args);
    executor.shutdown();
    try {
      if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
    }

    CanaryTool.Sink sink = canary.getActiveSink();
    assertEquals("verify canary timed out with TIMEOUT_ERROR_EXIT_CODE", 3, retCode);
    assertEquals("verify only the first region failed", 1, sink.getReadFailureCount());
    assertEquals("verify no successful reads", 0, sink.getReadSuccessCount());
    assertEquals("verify we were attempting to scan all regions", regions.size(),
      ((CanaryTool.RegionStdOutSink) sink).getTotalExpectedRegions());
  }

  @Test
  public void testCanaryRegionTaskReadAllCF() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    Table table = testingUtility.createTable(tableName,
      new byte[][] { Bytes.toBytes("f1"), Bytes.toBytes("f2") });
    // insert some test rows
    for (int i = 0; i < 1000; i++) {
      byte[] iBytes = Bytes.toBytes(i);
      Put p = new Put(iBytes);
      p.addColumn(Bytes.toBytes("f1"), COLUMN, iBytes);
      p.addColumn(Bytes.toBytes("f2"), COLUMN, iBytes);
      table.put(p);
    }
    Configuration configuration = HBaseConfiguration.create(testingUtility.getConfiguration());
    String[] args = { "-t", "10000", "testCanaryRegionTaskReadAllCF" };
    ExecutorService executor = new ScheduledThreadPoolExecutor(1);
    for (boolean readAllCF : new boolean[] { true, false }) {
      CanaryTool.RegionStdOutSink sink = spy(new CanaryTool.RegionStdOutSink());
      CanaryTool canary = new CanaryTool(executor, sink);
      configuration.setBoolean(HConstants.HBASE_CANARY_READ_ALL_CF, readAllCF);
      assertEquals(0, ToolRunner.run(configuration, canary, args));
      // the test table has two column family. If readAllCF set true,
      // we expect read count is double of region count
      int expectedReadCount =
        readAllCF ? 2 * sink.getTotalExpectedRegions() : sink.getTotalExpectedRegions();
      assertEquals("canary region success count should equal total expected read count",
        expectedReadCount, sink.getReadSuccessCount());
      Map<String, List<CanaryTool.RegionTaskResult>> regionMap = sink.getRegionMap();
      assertFalse("verify region map has size > 0", regionMap.isEmpty());

      for (String regionName : regionMap.keySet()) {
        for (CanaryTool.RegionTaskResult res : regionMap.get(regionName)) {
          assertNotNull("verify getRegionNameAsString()", regionName);
          assertNotNull("verify getRegionInfo()", res.getRegionInfo());
          assertNotNull("verify getTableName()", res.getTableName());
          assertNotNull("verify getTableNameAsString()", res.getTableNameAsString());
          assertNotNull("verify getServerName()", res.getServerName());
          assertNotNull("verify getServerNameAsString()", res.getServerNameAsString());
          assertNotNull("verify getColumnFamily()", res.getColumnFamily());
          assertNotNull("verify getColumnFamilyNameAsString()", res.getColumnFamilyNameAsString());
          assertTrue("read from region " + regionName + " succeeded", res.isReadSuccess());
          assertTrue("read took some time", res.getReadLatency() > -1);
        }
      }
    }
  }

  @Test
  public void testCanaryRegionTaskResult() throws Exception {
    TableName tableName = TableName.valueOf("testCanaryRegionTaskResult");
    Table table = testingUtility.createTable(tableName, new byte[][] { FAMILY });
    // insert some test rows
    for (int i = 0; i < 1000; i++) {
      byte[] iBytes = Bytes.toBytes(i);
      Put p = new Put(iBytes);
      p.addColumn(FAMILY, COLUMN, iBytes);
      table.put(p);
    }
    ExecutorService executor = new ScheduledThreadPoolExecutor(1);
    CanaryTool.RegionStdOutSink sink = spy(new CanaryTool.RegionStdOutSink());
    CanaryTool canary = new CanaryTool(executor, sink);
    String[] args = { "-writeSniffing", "-t", "10000", "testCanaryRegionTaskResult" };
    assertEquals(0, ToolRunner.run(testingUtility.getConfiguration(), canary, args));

    assertTrue("canary should expect to scan at least 1 region",
      sink.getTotalExpectedRegions() > 0);
    assertTrue("there should be no read failures", sink.getReadFailureCount() == 0);
    assertTrue("there should be no write failures", sink.getWriteFailureCount() == 0);
    assertTrue("verify read success count > 0", sink.getReadSuccessCount() > 0);
    assertTrue("verify write success count > 0", sink.getWriteSuccessCount() > 0);
    verify(sink, atLeastOnce()).publishReadTiming(isA(ServerName.class), isA(RegionInfo.class),
      isA(ColumnFamilyDescriptor.class), anyLong());
    verify(sink, atLeastOnce()).publishWriteTiming(isA(ServerName.class), isA(RegionInfo.class),
      isA(ColumnFamilyDescriptor.class), anyLong());

    assertEquals("canary region success count should equal total expected regions",
      sink.getReadSuccessCount() + sink.getWriteSuccessCount(), sink.getTotalExpectedRegions());
    Map<String, List<CanaryTool.RegionTaskResult>> regionMap = sink.getRegionMap();
    assertFalse("verify region map has size > 0", regionMap.isEmpty());

    for (String regionName : regionMap.keySet()) {
      for (CanaryTool.RegionTaskResult res : regionMap.get(regionName)) {
        assertNotNull("verify getRegionNameAsString()", regionName);
        assertNotNull("verify getRegionInfo()", res.getRegionInfo());
        assertNotNull("verify getTableName()", res.getTableName());
        assertNotNull("verify getTableNameAsString()", res.getTableNameAsString());
        assertNotNull("verify getServerName()", res.getServerName());
        assertNotNull("verify getServerNameAsString()", res.getServerNameAsString());
        assertNotNull("verify getColumnFamily()", res.getColumnFamily());
        assertNotNull("verify getColumnFamilyNameAsString()", res.getColumnFamilyNameAsString());

        if (regionName.contains(CanaryTool.DEFAULT_WRITE_TABLE_NAME.getNameAsString())) {
          assertTrue("write to region " + regionName + " succeeded", res.isWriteSuccess());
          assertTrue("write took some time", res.getWriteLatency() > -1);
        } else {
          assertTrue("read from region " + regionName + " succeeded", res.isReadSuccess());
          assertTrue("read took some time", res.getReadLatency() > -1);
        }
      }
    }
  }

  // Ignore this test. It fails w/ the below on some mac os x.
  // [ERROR] Failures:
  // [ERROR] TestCanaryTool.testReadTableTimeouts:216
  // Argument(s) are different! Wanted:
  // mockAppender.doAppend(
  // <custom argument matcher>
  // );
  // -> at org.apache.hadoop.hbase.tool.TestCanaryTool
  // .testReadTableTimeouts(TestCanaryTool.java:216)
  // Actual invocations have different arguments:
  // mockAppender.doAppend(
  // org.apache.log4j.spi.LoggingEvent@2055cfc1
  // );
  // )
  // )
  //
  @org.junit.Ignore
  @Test
  public void testReadTableTimeouts() throws Exception {
    final TableName[] tableNames = new TableName[] { TableName.valueOf(name.getMethodName() + "1"),
      TableName.valueOf(name.getMethodName() + "2") };
    // Create 2 test tables.
    for (int j = 0; j < 2; j++) {
      Table table = testingUtility.createTable(tableNames[j], new byte[][] { FAMILY });
      // insert some test rows
      for (int i = 0; i < 10; i++) {
        byte[] iBytes = Bytes.toBytes(i + j);
        Put p = new Put(iBytes);
        p.addColumn(FAMILY, COLUMN, iBytes);
        table.put(p);
      }
    }
    ExecutorService executor = new ScheduledThreadPoolExecutor(1);
    CanaryTool.RegionStdOutSink sink = spy(new CanaryTool.RegionStdOutSink());
    CanaryTool canary = new CanaryTool(executor, sink);
    String configuredTimeoutStr = tableNames[0].getNameAsString() + "=" + Long.MAX_VALUE + ","
      + tableNames[1].getNameAsString() + "=0";
    String[] args = { "-readTableTimeouts", configuredTimeoutStr, name.getMethodName() + "1",
      name.getMethodName() + "2" };
    assertEquals(0, ToolRunner.run(testingUtility.getConfiguration(), canary, args));
    verify(sink, times(tableNames.length)).initializeAndGetReadLatencyForTable(isA(String.class));
    for (int i = 0; i < 2; i++) {
      assertNotEquals("verify non-null read latency", null,
        sink.getReadLatencyMap().get(tableNames[i].getNameAsString()));
      assertNotEquals("verify non-zero read latency", 0L,
        sink.getReadLatencyMap().get(tableNames[i].getNameAsString()));
    }
    // One table's timeout is set for 0 ms and thus, should lead to an error.
    verify(mockAppender, times(1))
      .append(argThat(new ArgumentMatcher<org.apache.logging.log4j.core.LogEvent>() {
        @Override
        public boolean matches(org.apache.logging.log4j.core.LogEvent argument) {
          return argument.getMessage().getFormattedMessage()
            .contains("exceeded the configured read timeout.");
        }
      }));
    verify(mockAppender, times(2))
      .append(argThat(new ArgumentMatcher<org.apache.logging.log4j.core.LogEvent>() {
        @Override
        public boolean matches(org.apache.logging.log4j.core.LogEvent argument) {
          return argument.getMessage().getFormattedMessage().contains("Configured read timeout");
        }
      }));
  }

  @Test
  public void testWriteTableTimeout() throws Exception {
    ExecutorService executor = new ScheduledThreadPoolExecutor(1);
    CanaryTool.RegionStdOutSink sink = spy(new CanaryTool.RegionStdOutSink());
    CanaryTool canary = new CanaryTool(executor, sink);
    String[] args = { "-writeSniffing", "-writeTableTimeout", String.valueOf(Long.MAX_VALUE) };
    assertEquals(0, ToolRunner.run(testingUtility.getConfiguration(), canary, args));
    assertNotEquals("verify non-null write latency", null, sink.getWriteLatency());
    assertNotEquals("verify non-zero write latency", 0L, sink.getWriteLatency());
    verify(mockAppender, times(1))
      .append(argThat(new ArgumentMatcher<org.apache.logging.log4j.core.LogEvent>() {
        @Override
        public boolean matches(org.apache.logging.log4j.core.LogEvent argument) {
          return argument.getMessage().getFormattedMessage().contains("Configured write timeout");
        }
      }));
  }

  // no table created, so there should be no regions
  @Test
  public void testRegionserverNoRegions() throws Exception {
    runRegionserverCanary();
    verify(mockAppender)
      .append(argThat(new ArgumentMatcher<org.apache.logging.log4j.core.LogEvent>() {
        @Override
        public boolean matches(org.apache.logging.log4j.core.LogEvent argument) {
          return argument.getMessage().getFormattedMessage()
            .contains("Regionserver not serving any regions");
        }
      }));
  }

  // by creating a table, there shouldn't be any region servers not serving any regions
  @Test
  public void testRegionserverWithRegions() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    testingUtility.createTable(tableName, new byte[][] { FAMILY });
    runRegionserverCanary();
    verify(mockAppender, never())
      .append(argThat(new ArgumentMatcher<org.apache.logging.log4j.core.LogEvent>() {
        @Override
        public boolean matches(org.apache.logging.log4j.core.LogEvent argument) {
          return argument.getMessage().getFormattedMessage()
            .contains("Regionserver not serving any regions");
        }
      }));
  }

  @Test
  public void testRawScanConfig() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    Table table = testingUtility.createTable(tableName, new byte[][] { FAMILY });
    // insert some test rows
    for (int i = 0; i < 1000; i++) {
      byte[] iBytes = Bytes.toBytes(i);
      Put p = new Put(iBytes);
      p.addColumn(FAMILY, COLUMN, iBytes);
      table.put(p);
    }
    ExecutorService executor = new ScheduledThreadPoolExecutor(1);
    CanaryTool.RegionStdOutSink sink = spy(new CanaryTool.RegionStdOutSink());
    CanaryTool canary = new CanaryTool(executor, sink);
    String[] args = { "-t", "10000", name.getMethodName() };
    org.apache.hadoop.conf.Configuration conf =
      new org.apache.hadoop.conf.Configuration(testingUtility.getConfiguration());
    conf.setBoolean(HConstants.HBASE_CANARY_READ_RAW_SCAN_KEY, true);
    assertEquals(0, ToolRunner.run(conf, canary, args));
    verify(sink, atLeastOnce()).publishReadTiming(isA(ServerName.class), isA(RegionInfo.class),
      isA(ColumnFamilyDescriptor.class), anyLong());
    assertEquals("verify no read error count", 0, canary.getReadFailures().size());
  }

  private void runRegionserverCanary() throws Exception {
    ExecutorService executor = new ScheduledThreadPoolExecutor(1);
    CanaryTool canary = new CanaryTool(executor, new CanaryTool.RegionServerStdOutSink());
    String[] args = { "-t", "10000", "-regionserver" };
    assertEquals(0, ToolRunner.run(testingUtility.getConfiguration(), canary, args));
    assertEquals("verify no read error count", 0, canary.getReadFailures().size());
  }

  private void testZookeeperCanaryWithArgs(String[] args) throws Exception {
    String hostPort = testingUtility.getZkCluster().getAddress().toString();
    testingUtility.getConfiguration().set(HConstants.ZOOKEEPER_QUORUM, hostPort + "/hbase");
    ExecutorService executor = new ScheduledThreadPoolExecutor(2);
    CanaryTool.ZookeeperStdOutSink sink = spy(new CanaryTool.ZookeeperStdOutSink());
    CanaryTool canary = new CanaryTool(executor, sink);
    assertEquals(0, ToolRunner.run(testingUtility.getConfiguration(), canary, args));

    String baseZnode = testingUtility.getConfiguration().get(HConstants.ZOOKEEPER_ZNODE_PARENT,
      HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    verify(sink, atLeastOnce()).publishReadTiming(eq(baseZnode), eq(hostPort), anyLong());
  }
}
