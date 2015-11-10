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
package edu.umass.cs.gnsserver.nodeconfig;

import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentHashing;

import java.net.InetAddress;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;

/**
 * A wrapper around NodeConfig to ensure that it is
 * consistent, i.e., it returns consistent results even if it changes
 * midway. 
 * In particular, it does not allow the use of a method like
 * getNodeIDs().
 *
 * @param <NodeIDType>
 */
public class GNSConsistentNodeConfig<NodeIDType> implements
        GNSInterfaceNodeConfig<NodeIDType> {

  private final GNSInterfaceNodeConfig<NodeIDType> nodeConfig;
  private Set<NodeIDType> nodes; // most recent cached copy

  private final ConsistentHashing<NodeIDType> CH; // need to refresh when nodeConfig changes

  /**
   * Creates a GNSConsistentNodeConfig instance.
   * 
   * @param nc
   */
  public GNSConsistentNodeConfig(GNSInterfaceNodeConfig<NodeIDType> nc) {
    this.nodeConfig = nc;
    this.nodes = this.nodeConfig.getNodeIDs();
    this.CH = new ConsistentHashing<NodeIDType>(this.nodes);
  }

  private synchronized boolean refresh() {
    Set<NodeIDType> curActives = this.nodeConfig.getNodeIDs();
    if (curActives.equals(this.nodes)) {
      return false;
    }
    this.nodes = (curActives);
    this.CH.refresh(curActives);
    return true;
  }

  @Override
  public Set<NodeIDType> getActiveReplicas() {
    return this.nodeConfig.getActiveReplicas();
  }

  @Override
  public Set<NodeIDType> getReconfigurators() {
    return this.nodeConfig.getReconfigurators();
  }

  /**
   * Returns replicated servers.
   * 
   * @param name
   * @return a set of node ids
   */
  public Set<NodeIDType> getReplicatedServers(String name) {
    refresh();
    return this.CH.getReplicatedServers(name);
  }

  @Override
  public boolean nodeExists(NodeIDType id) {
    return this.nodeConfig.nodeExists(id);
  }

  @Override
  public InetAddress getNodeAddress(NodeIDType id) {
    return this.nodeConfig.getNodeAddress(id);
  }

  @Override
  public InetAddress getBindAddress(NodeIDType id) {
    return this.nodeConfig.getBindAddress(id);
  }

  @Override
  public int getNodePort(NodeIDType id) {
    return this.nodeConfig.getNodePort(id);
  }

  @Override
  public int getAdminPort(NodeIDType id) {
    return this.nodeConfig.getAdminPort(id);
  }

  @Override
  public int getPingPort(NodeIDType id) {
    return this.nodeConfig.getPingPort(id);
  }

  @Override
  public int getCcpPort(NodeIDType id) {
    return this.nodeConfig.getCcpPort(id);
  }

  @Override
  public int getCcpAdminPort(NodeIDType id) {
    return this.nodeConfig.getCcpAdminPort(id);
  }

  @Override
  public int getCcpPingPort(NodeIDType id) {
    return this.nodeConfig.getCcpPingPort(id);
  }

  @Override
  public long getPingLatency(NodeIDType id) {
    return this.nodeConfig.getPingLatency(id);
  }

  @Override
  public void updatePingLatency(NodeIDType id, long responseTime) {
    this.nodeConfig.updatePingLatency(id, responseTime);
  }

  // FIXME: disallow the use of this method
  @Override
  @Deprecated
  public Set<NodeIDType> getNodeIDs() {
    throw new RuntimeException("The use of this method is not permitted");
    //return this.nodeConfig.getNodeIDs();
  }

  @Override
  public NodeIDType valueOf(String strValue) {
    return this.nodeConfig.valueOf(strValue);
  }

  @Override
  public Set<NodeIDType> getValuesFromStringSet(Set<String> strNodes) {
    return this.nodeConfig.getValuesFromStringSet(strNodes);
  }

  @Override
  public Set<NodeIDType> getValuesFromJSONArray(JSONArray array)
          throws JSONException {
    return this.nodeConfig.getValuesFromJSONArray(array);
  }

  @Override
  public long getVersion() {
    return this.nodeConfig.getVersion();
  }

}
