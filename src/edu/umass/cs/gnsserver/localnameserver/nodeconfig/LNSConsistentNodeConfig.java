/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.localnameserver.nodeconfig;


import edu.umass.cs.gnsserver.nodeconfig.GNSInterfaceNodeConfig;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentHashing;

/**
 * @author westy
 */
public abstract class LNSConsistentNodeConfig implements
        GNSInterfaceNodeConfig<InetSocketAddress> {

  private final NodeConfig<InetSocketAddress> nodeConfig;
  private Set<InetSocketAddress> nodes; // most recent cached copy

  private final ConsistentHashing<InetSocketAddress> CH; // need to refresh when nodeConfig changes

  /**
   * Creates a LNSConsistentNodeConfig instance.
   * 
   * @param nc
   */
  public LNSConsistentNodeConfig(NodeConfig<InetSocketAddress> nc) {
    this.nodeConfig = nc;
    this.nodes = this.nodeConfig.getNodeIDs();
    this.CH = new ConsistentHashing<>(this.nodes);
  }

  private synchronized boolean refresh() {
    Set<InetSocketAddress> curActives = this.nodeConfig.getNodeIDs();
    if (curActives.equals(this.nodes)) {
      return false;
    }
    this.nodes = (curActives);
    this.CH.refresh(curActives);
    return true;
  }

  /**
   * Returns the list of replicated servers.
   * 
   * @param name
   * @return a set of addresses
   */
  public Set<InetSocketAddress> getReplicatedServers(String name) {
    refresh();
    return this.CH.getReplicatedServers(name);
  }

  /**
   *
   * @param id
   * @return true if the node exists
   */
  @Override
  public boolean nodeExists(InetSocketAddress id) {
    return this.nodeConfig.nodeExists(id);
  }

  /**
   *
   * @param id
   * @return the node address
   */
  @Override
  public InetAddress getNodeAddress(InetSocketAddress id) {
    return this.nodeConfig.getNodeAddress(id);
  }

  /**
   *
   * @param id
   * @return the band address
   */
  @Override
  public InetAddress getBindAddress(InetSocketAddress id) {
    throw new RuntimeException("The use of this method is not permitted");
  }

  /**
   *
   * @param id
   * @return the port
   */
  @Override
  public int getNodePort(InetSocketAddress id) {
    return this.nodeConfig.getNodePort(id);
  }

  /**
   *
   * @return a set of address
   */
  @Override
  public Set<InetSocketAddress> getNodeIDs() {
    throw new RuntimeException("The use of this method is not permitted");
    //return this.nodeConfig.getNodeIDs();
  }

  /**
   *
   * @param strValue
   * @return the address
   */
  @Override
  public InetSocketAddress valueOf(String strValue) {
    return this.nodeConfig.valueOf(strValue);
  }

  /**
   *
   * @param strNodes
   * @return a set of addresses
   */
  @Override
  public Set<InetSocketAddress> getValuesFromStringSet(Set<String> strNodes) {
    return this.nodeConfig.getValuesFromStringSet(strNodes);
  }

  /**
   *
   * @param array
   * @return a set of addresses
   * @throws JSONException
   */
  @Override
  public Set<InetSocketAddress> getValuesFromJSONArray(JSONArray array)
          throws JSONException {
    return this.nodeConfig.getValuesFromJSONArray(array);
  }
}
