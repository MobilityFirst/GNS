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

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import edu.umass.cs.reconfiguration.interfaces.ModifiableActiveConfig;
import edu.umass.cs.reconfiguration.interfaces.ModifiableRCConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentHashing;

/**
 * This class is a wrapper around NodeConfig to ensure that it is consistent,
 * i.e., it returns consistent results even if it changes midway. In particular,
 * it does not allow the use of a method like getNodeIDs().
 *
 * It also has consistent hashing utility methods.
 *
 */
public class LNSConsistentReconfigurableNodeConfig extends
        LNSConsistentNodeConfig implements
        ModifiableActiveConfig<InetSocketAddress>, ModifiableRCConfig<InetSocketAddress> {

  private final LNSNodeConfig nodeConfig;
  private Set<InetSocketAddress> activeReplicas; // most recent cached copy
  private Set<InetSocketAddress> reconfigurators; // most recent cached copy

  // need to refresh when nodeConfig changes
  private final ConsistentHashing<InetSocketAddress> CH_RC;
  // need to refresh when nodeConfig changes
  private final ConsistentHashing<InetSocketAddress> CH_AR;

  private Set<InetSocketAddress> reconfiguratorsSlatedForRemoval = new HashSet<InetSocketAddress>();

  /**
   * Create a LNSConsistentReconfigurableNodeConfig instance.
   *
   * @param nc
   */
  public LNSConsistentReconfigurableNodeConfig(
          LNSNodeConfig nc) {
    super(nc);
    this.nodeConfig = nc;
    this.activeReplicas = this.nodeConfig.getActiveReplicas();
    this.reconfigurators = this.nodeConfig.getReconfigurators();
    this.CH_RC = new ConsistentHashing<>(this.reconfigurators);
    this.CH_AR = new ConsistentHashing<>(this.activeReplicas); 
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

  /**
   *
   * @return a set of addresses
   */
  @Override
  public Set<InetSocketAddress> getNodeIDs() {
    return this.nodeConfig.getNodeIDs();
    //throw new RuntimeException("The use of this method is not permitted");
  }

  /**
   *
   * @return a set of addresses
   */
  @Override
  public Set<InetSocketAddress> getActiveReplicas() {
    return this.nodeConfig.getActiveReplicas();
  }

  /**
   *
   * @return a set of addresses
   */
  @Override
  public Set<InetSocketAddress> getReconfigurators() {
    return this.nodeConfig.getReconfigurators();
  }

  /**
   * Returns the set of reconfigurators.
   *
   * @param name
   * @return a set of addresses
   */
  public Set<InetSocketAddress> getReplicatedReconfigurators(String name) {
    // refresh before returning
    this.refreshReconfigurators();
    return this.CH_RC.getReplicatedServers(name);
  }

  
  /**
   * Returns the set of active replicas.
   * 
   * @param name
   * @return a set of addresses
   */
  public Set<InetSocketAddress> getReplicatedActives(String name) {
    // refresh before returning
    this.refreshActives();
    return this.CH_AR.getReplicatedServers(name);
  }

  // refresh consistent hash structure if changed
  private synchronized boolean refreshActives() {
    Set<InetSocketAddress> curActives = this.nodeConfig.getActiveReplicas();
    if (curActives.equals(this.getLastActives())) {
      return false;
    }
    this.setLastActives(curActives);
    this.CH_AR.refresh(curActives);
    return true;
  }

  // refresh consistent hash structure if changed
  private synchronized boolean refreshReconfigurators() {
    Set<InetSocketAddress> curReconfigurators = this.nodeConfig
            .getReconfigurators();
    if (curReconfigurators.equals(this.getLastReconfigurators())) {
      return false;
    }
    this.setLastReconfigurators(curReconfigurators);
    this.CH_RC.refresh(curReconfigurators);
    return true;
  }

  private synchronized Set<InetSocketAddress> getLastActives() {
    return this.activeReplicas;
  }

  private synchronized Set<InetSocketAddress> getLastReconfigurators() {
    return this.reconfigurators;
  }

  private synchronized Set<InetSocketAddress> setLastActives(
          Set<InetSocketAddress> curActives) {
    return this.activeReplicas = curActives;
  }

  private synchronized Set<InetSocketAddress> setLastReconfigurators(
          Set<InetSocketAddress> curReconfigurators) {
    return this.reconfigurators = curReconfigurators;
  }

  /**
   *
   * @param id
   * @param sockAddr
   * @return an address
   */
  @Override
  public InetSocketAddress addReconfigurator(InetSocketAddress id,
          InetSocketAddress sockAddr) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  /**
   *
   * @param id
   * @return an address
   */
  @Override
  public InetSocketAddress removeReconfigurator(InetSocketAddress id) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  /**
   *
   * @param id
   * @param sockAddr
   * @return an address
   */
  @Override
  public InetSocketAddress addActiveReplica(InetSocketAddress id,
          InetSocketAddress sockAddr) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  /**
   *
   * @param id
   * @return an address
   */
  @Override
  public InetSocketAddress removeActiveReplica(InetSocketAddress id) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public long getVersion() {
    return this.nodeConfig.getVersion();
  }

  @Override
  public int getServerAdminPort(InetSocketAddress id) {
    return this.nodeConfig.getServerAdminPort(id);
  }

  @Override
  public int getCollatingAdminPort(InetSocketAddress id) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public long getPingLatency(InetSocketAddress id) {
    return this.nodeConfig.getPingLatency(id);
  }

  @Override
  public void updatePingLatency(InetSocketAddress id, long responseTime) {
    this.nodeConfig.updatePingLatency(id, responseTime);
  }

}
