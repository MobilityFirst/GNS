package edu.umass.cs.gns.localnameserver.nodeconfig;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.reconfiguration.interfaces.InterfaceModifiableActiveConfig;
import edu.umass.cs.reconfiguration.interfaces.InterfaceModifiableRCConfig;
import edu.umass.cs.reconfiguration.interfaces.InterfaceReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentHashing;
import edu.umass.cs.reconfiguration.reconfigurationutils.SimpleReconfiguratorNodeConfig;

/*
 * This class is a wrapper around NodeConfig to ensure that it is consistent,
 * i.e., it returns consistent results even if it changes midway. In particular,
 * it does not allow the use of a method like getNodeIDs().
 * 
 * It also has consistent hashing utility methods.
 */
public class LNSConsistentReconfigurableNodeConfig extends
        LNSConsistentNodeConfig implements
        InterfaceModifiableActiveConfig<InetSocketAddress>, InterfaceModifiableRCConfig<InetSocketAddress> {

  private final LNSNodeConfig nodeConfig;
  private Set<InetSocketAddress> activeReplicas; // most recent cached copy
  private Set<InetSocketAddress> reconfigurators; // most recent cached copy

  // need to refresh when nodeConfig changes
  private final ConsistentHashing<InetSocketAddress> CH_RC;
  // need to refresh when nodeConfig changes
  private final ConsistentHashing<InetSocketAddress> CH_AR;

  private Set<InetSocketAddress> reconfiguratorsSlatedForRemoval = new HashSet<InetSocketAddress>();

  public LNSConsistentReconfigurableNodeConfig(
          LNSNodeConfig nc) {
    super(nc);
    this.nodeConfig = nc;
    this.activeReplicas = this.nodeConfig.getActiveReplicas();
    this.reconfigurators = this.nodeConfig.getReconfigurators();
    this.CH_RC = new ConsistentHashing<InetSocketAddress>(this.reconfigurators);
    this.CH_AR = new ConsistentHashing<InetSocketAddress>(this.activeReplicas, true);
  }

  @Override
  public Set<InetSocketAddress> getValuesFromStringSet(Set<String> strNodes) {
    return this.nodeConfig.getValuesFromStringSet(strNodes);
  }

  @Override
  public Set<InetSocketAddress> getValuesFromJSONArray(JSONArray array)
          throws JSONException {
    return this.nodeConfig.getValuesFromJSONArray(array);
  }

  @Override
  public Set<InetSocketAddress> getNodeIDs() {
    throw new RuntimeException("The use of this method is not permitted");
  }

  @Override
  public Set<InetSocketAddress> getActiveReplicas() {
    return this.nodeConfig.getActiveReplicas();
  }

  @Override
  public Set<InetSocketAddress> getReconfigurators() {
    return this.nodeConfig.getReconfigurators();
  }

  // consistent coz it always consults nodeConfig
  public ArrayList<InetAddress> getNodeIPs(Set<InetSocketAddress> nodeIDs) {
    ArrayList<InetAddress> addresses = new ArrayList<InetAddress>();
    for (InetSocketAddress id : nodeIDs) {
      addresses.add(this.nodeConfig.getNodeAddress(id));
    }
    assert (addresses != null);
    return addresses;
  }

  // refresh before returning
  public Set<InetSocketAddress> getReplicatedReconfigurators(String name) {
    this.refreshReconfigurators();
    return this.CH_RC.getReplicatedServers(name);
  }

  // refresh before returning
  public Set<InetSocketAddress> getReplicatedActives(String name) {
    this.refreshActives();
    return this.CH_AR.getReplicatedServers(name);
  }
//
//  public ArrayList<InetAddress> getReplicatedActivesIPs(String name) {
//    return this.getNodeIPs(this.getReplicatedActives(name));
//  }

  public InterfaceReconfigurableNodeConfig<InetSocketAddress> getUnderlyingNodeConfig() {
    return this.nodeConfig;
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

  @Override
  public InetSocketAddress addReconfigurator(InetSocketAddress id,
          InetSocketAddress sockAddr) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public InetSocketAddress removeReconfigurator(InetSocketAddress id) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public InetSocketAddress slateForRemovalReconfigurator(InetSocketAddress id) {
    this.reconfiguratorsSlatedForRemoval.add(id);
    return this.getNodeSocketAddress(id);
  }

  @Override
  public InetSocketAddress addActiveReplica(InetSocketAddress id,
          InetSocketAddress sockAddr) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public InetSocketAddress removeActiveReplica(InetSocketAddress id) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public long getVersion() {
    return this.nodeConfig.getVersion();
  }

  @Override
  public int getAdminPort(InetSocketAddress id) {
    return this.nodeConfig.getAdminPort(id);
  }

  @Override
  public int getPingPort(InetSocketAddress id) {
    return this.nodeConfig.getPingPort(id);
  }

  @Override
  public int getCcpPort(InetSocketAddress id) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int getCcpPingPort(InetSocketAddress id) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int getCcpAdminPort(InetSocketAddress id) {
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
