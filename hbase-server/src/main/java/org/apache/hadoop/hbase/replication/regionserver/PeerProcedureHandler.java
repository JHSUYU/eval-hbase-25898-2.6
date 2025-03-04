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
package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A handler for modifying replication peer in peer procedures.
 */
@InterfaceAudience.Private
public interface PeerProcedureHandler {

  public void addPeer(String peerId) throws ReplicationException, IOException;

  public void removePeer(String peerId) throws ReplicationException, IOException;

  public void disablePeer(String peerId) throws ReplicationException, IOException;

  public void enablePeer(String peerId) throws ReplicationException, IOException;

  public void updatePeerConfig(String peerId) throws ReplicationException, IOException;

  void claimReplicationQueue(ServerName crashedServer, String queue)
    throws ReplicationException, IOException;
}
