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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.assignment.AssignmentTestingUtil;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ClientTests.class, MediumTests.class })
public class TestSeparateClientZKCluster {
  private static final Logger LOG = LoggerFactory.getLogger(TestSeparateClientZKCluster.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final File clientZkDir =
    new File(TEST_UTIL.getDataTestDir("TestSeparateClientZKCluster").toString());
  private static final int ZK_SESSION_TIMEOUT = 5000;
  private static MiniZooKeeperCluster clientZkCluster;

  private final byte[] family = Bytes.toBytes("cf");
  private final byte[] qualifier = Bytes.toBytes("c1");
  private final byte[] row = Bytes.toBytes("row");
  private final byte[] value = Bytes.toBytes("v1");
  private final byte[] newVal = Bytes.toBytes("v2");

  @Rule
  public TableNameTestRule name = new TableNameTestRule();

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSeparateClientZKCluster.class);

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    int clientZkPort = 21828;
    clientZkCluster = new MiniZooKeeperCluster(TEST_UTIL.getConfiguration());
    clientZkCluster.setDefaultClientPort(clientZkPort);
    clientZkCluster.startup(clientZkDir);
    // start log counter
    TEST_UTIL.getConfiguration().setInt("hbase.client.start.log.errors.counter", 3);
    TEST_UTIL.getConfiguration().setInt("zookeeper.recovery.retry", 1);
    // core settings for testing client ZK cluster
    TEST_UTIL.getConfiguration().setClass(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
      ZKConnectionRegistry.class, ConnectionRegistry.class);
    TEST_UTIL.getConfiguration().set(HConstants.CLIENT_ZOOKEEPER_QUORUM, HConstants.LOCALHOST);
    TEST_UTIL.getConfiguration().setInt(HConstants.CLIENT_ZOOKEEPER_CLIENT_PORT, clientZkPort);
    // reduce zk session timeout to easier trigger session expiration
    TEST_UTIL.getConfiguration().setInt(HConstants.ZK_SESSION_TIMEOUT, ZK_SESSION_TIMEOUT);
    // Start a cluster with 2 masters and 3 regionservers.
    StartMiniClusterOption option =
      StartMiniClusterOption.builder().numMasters(2).numRegionServers(3).numDataNodes(3).build();
    TEST_UTIL.startMiniCluster(option);
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    clientZkCluster.shutdown();
    FileUtils.deleteDirectory(clientZkDir);
  }

  @Before
  public void setUp() throws IOException {
    try (Admin admin = TEST_UTIL.getConnection().getAdmin()) {
      waitForNewMasterUpAndAddressSynced(admin);
    }
  }

  private void waitForNewMasterUpAndAddressSynced(Admin admin) {
    TEST_UTIL.waitFor(30000, () -> {
      try {
        return admin.listNamespaces().length > 0;
      } catch (Exception e) {
        LOG.warn("failed to list namespaces", e);
        return false;
      }
    });
  }

  @Test
  public void testBasicOperation() throws Exception {
    TableName tn = name.getTableName();
    // create table
    Connection conn = TEST_UTIL.getConnection();
    try (Admin admin = conn.getAdmin(); Table table = conn.getTable(tn)) {
      ColumnFamilyDescriptorBuilder cfDescBuilder =
        ColumnFamilyDescriptorBuilder.newBuilder(family);
      TableDescriptorBuilder tableDescBuilder =
        TableDescriptorBuilder.newBuilder(tn).setColumnFamily(cfDescBuilder.build());
      admin.createTable(tableDescBuilder.build());
      // test simple get and put
      Put put = new Put(row);
      put.addColumn(family, qualifier, value);
      table.put(put);
      Get get = new Get(row);
      Result result = table.get(get);
      LOG.debug("Result: " + Bytes.toString(result.getValue(family, qualifier)));
      assertArrayEquals(value, result.getValue(family, qualifier));
    }
  }

  @Test
  public void testMasterSwitch() throws Exception {
    // get an admin instance and issue some request first
    Connection conn = TEST_UTIL.getConnection();
    try (Admin admin = conn.getAdmin()) {
      LOG.debug("Tables: " + admin.listTableDescriptors());
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      // switch active master
      HMaster master = cluster.getMaster();
      master.stopMaster();
      LOG.info("Stopped master {}", master.getServerName());
      TEST_UTIL.waitFor(30000, () -> !master.isAlive());
      LOG.info("Shutdown master {}", master.getServerName());
      TEST_UTIL.waitFor(30000,
        () -> cluster.getMaster() != null && cluster.getMaster().isInitialized());
      LOG.info("Got master {}", cluster.getMaster().getServerName());
      // confirm client access still works
      waitForNewMasterUpAndAddressSynced(admin);
    }
  }

  @Test
  public void testMetaRegionMove() throws Exception {
    TableName tn = name.getTableName();
    // create table
    Connection conn = TEST_UTIL.getConnection();
    try (Admin admin = conn.getAdmin(); Table table = conn.getTable(tn);
      RegionLocator locator = conn.getRegionLocator(tn)) {
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      ColumnFamilyDescriptorBuilder cfDescBuilder =
        ColumnFamilyDescriptorBuilder.newBuilder(family);
      TableDescriptorBuilder tableDescBuilder =
        TableDescriptorBuilder.newBuilder(tn).setColumnFamily(cfDescBuilder.build());
      admin.createTable(tableDescBuilder.build());
      // issue some requests to cache the region location
      Put put = new Put(row);
      put.addColumn(family, qualifier, value);
      table.put(put);
      Get get = new Get(row);
      Result result = table.get(get);
      // move meta region and confirm client could detect
      ServerName destServerName = null;
      for (RegionServerThread rst : cluster.getLiveRegionServerThreads()) {
        ServerName name = rst.getRegionServer().getServerName();
        if (!name.equals(cluster.getServerHoldingMeta())) {
          destServerName = name;
          break;
        }
      }
      admin.move(RegionInfoBuilder.FIRST_META_REGIONINFO.getEncodedNameAsBytes(), destServerName);
      LOG.debug("Finished moving meta");
      // invalidate client cache
      RegionInfo region = locator.getRegionLocation(row).getRegion();
      ServerName currentServer = cluster.getServerHoldingRegion(tn, region.getRegionName());
      for (RegionServerThread rst : cluster.getLiveRegionServerThreads()) {
        ServerName name = rst.getRegionServer().getServerName();
        if (!name.equals(currentServer)) {
          destServerName = name;
          break;
        }
      }
      admin.move(region.getEncodedNameAsBytes(), destServerName);
      LOG.debug("Finished moving user region");
      put = new Put(row);
      put.addColumn(family, qualifier, newVal);
      table.put(put);
      result = table.get(get);
      LOG.debug("Result: " + Bytes.toString(result.getValue(family, qualifier)));
      assertArrayEquals(newVal, result.getValue(family, qualifier));
    }
  }

  @Test
  public void testMetaMoveDuringClientZkClusterRestart() throws Exception {
    TableName tn = name.getTableName();
    // create table
    Connection conn = TEST_UTIL.getConnection();
    try (Admin admin = conn.getAdmin(); Table table = conn.getTable(tn)) {
      ColumnFamilyDescriptorBuilder cfDescBuilder =
        ColumnFamilyDescriptorBuilder.newBuilder(family);
      TableDescriptorBuilder tableDescBuilder =
        TableDescriptorBuilder.newBuilder(tn).setColumnFamily(cfDescBuilder.build());
      admin.createTable(tableDescBuilder.build());
      // put some data
      Put put = new Put(row);
      put.addColumn(family, qualifier, value);
      table.put(put);
      // invalid connection cache
      conn.clearRegionLocationCache();
      // stop client zk cluster
      clientZkCluster.shutdown();
      // stop current meta server and confirm the server shutdown process
      // is not affected by client ZK crash
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      int metaServerId = cluster.getServerWithMeta();
      HRegionServer metaServer = cluster.getRegionServer(metaServerId);
      metaServer.stop("Stop current RS holding meta region");
      while (!metaServer.isShutDown()) {
        Thread.sleep(200);
      }
      // wait for meta region online
      AssignmentTestingUtil.waitForAssignment(cluster.getMaster().getAssignmentManager(),
        RegionInfoBuilder.FIRST_META_REGIONINFO);
      // wait some long time to make sure we will retry sync data to client ZK until data set
      Thread.sleep(10000);
      clientZkCluster.startup(clientZkDir);
      // new request should pass
      Get get = new Get(row);
      Result result = table.get(get);
      LOG.debug("Result: " + Bytes.toString(result.getValue(family, qualifier)));
      assertArrayEquals(value, result.getValue(family, qualifier));
    }
  }

  @Test
  public void testAsyncTable() throws Exception {
    TableName tn = name.getTableName();
    ColumnFamilyDescriptorBuilder cfDescBuilder = ColumnFamilyDescriptorBuilder.newBuilder(family);
    TableDescriptorBuilder tableDescBuilder =
      TableDescriptorBuilder.newBuilder(tn).setColumnFamily(cfDescBuilder.build());
    try (AsyncConnection ASYNC_CONN =
      ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get()) {
      ASYNC_CONN.getAdmin().createTable(tableDescBuilder.build()).get();
      AsyncTable<?> table = ASYNC_CONN.getTable(tn);
      // put some data
      Put put = new Put(row);
      put.addColumn(family, qualifier, value);
      table.put(put).get();
      // get and verify
      Get get = new Get(row);
      Result result = table.get(get).get();
      LOG.debug("Result: " + Bytes.toString(result.getValue(family, qualifier)));
      assertArrayEquals(value, result.getValue(family, qualifier));
    }
  }

  @Test
  public void testChangeMetaReplicaCount() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    try (RegionLocator locator =
      TEST_UTIL.getConnection().getRegionLocator(TableName.META_TABLE_NAME)) {
      assertEquals(1, locator.getAllRegionLocations().size());
      HBaseTestingUtility.setReplicas(admin, TableName.META_TABLE_NAME, 3);
      TEST_UTIL.waitFor(30000, () -> locator.getAllRegionLocations().size() == 3);
      HBaseTestingUtility.setReplicas(admin, TableName.META_TABLE_NAME, 2);
      TEST_UTIL.waitFor(30000, () -> locator.getAllRegionLocations().size() == 2);
      HBaseTestingUtility.setReplicas(admin, TableName.META_TABLE_NAME, 1);
      TEST_UTIL.waitFor(30000, () -> locator.getAllRegionLocations().size() == 1);
    }
  }
}
