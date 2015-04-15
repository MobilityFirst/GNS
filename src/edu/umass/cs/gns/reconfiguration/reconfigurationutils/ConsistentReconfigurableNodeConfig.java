package edu.umass.cs.gns.reconfiguration.reconfigurationutils;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableNodeConfig;

/*
 * This class is a wrapper around NodeConfig to ensure that it is consistent,
 * i.e., it returns consistent results even if it changes midway. In particular,
 * it does not allow the use of a method like getNodeIDs().
 * 
 * It also has consistent hashing utility methods.
 */
public class ConsistentReconfigurableNodeConfig<NodeIDType> extends
        ConsistentNodeConfig<NodeIDType> implements
        InterfaceReconfigurableNodeConfig<NodeIDType> {

  private final InterfaceReconfigurableNodeConfig<NodeIDType> nodeConfig;
  private Set<NodeIDType> activeReplicas; // most recent cached copy
  private Set<NodeIDType> reconfigurators; // most recent cached copy

  // need to refresh when nodeConfig changes
  private final ConsistentHashing<NodeIDType> CH_RC;
  // need to refresh when nodeConfig changes
  private final ConsistentHashing<NodeIDType> CH_AR;

  public ConsistentReconfigurableNodeConfig(
          InterfaceReconfigurableNodeConfig<NodeIDType> nc) {
    super(nc);
    this.nodeConfig = nc;
    this.activeReplicas = this.nodeConfig.getActiveReplicas();
    this.reconfigurators = this.nodeConfig.getReconfigurators();
    this.CH_RC = new ConsistentHashing<NodeIDType>(this.reconfigurators);
    this.CH_AR = new ConsistentHashing<NodeIDType>(this.activeReplicas);
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
  @Deprecated
  public Set<NodeIDType> getNodeIDs() {
    throw new RuntimeException("The use of this method is not permitted");
  }

  @Override
  public Set<NodeIDType> getActiveReplicas() {
    return this.nodeConfig.getActiveReplicas();
  }

  @Override
  public Set<NodeIDType> getReconfigurators() {
    return this.nodeConfig.getReconfigurators();
  }

  // consistent coz it always consults nodeConfig
  public ArrayList<InetAddress> getNodeIPs(Set<NodeIDType> nodeIDs) {
    ArrayList<InetAddress> addresses = new ArrayList<InetAddress>();
    for (NodeIDType id : nodeIDs) {
      addresses.add(this.nodeConfig.getNodeAddress(id));
    }
    assert (addresses != null);
    return addresses;
  }

  // refresh before returning
  public Set<NodeIDType> getReplicatedReconfigurators(String name) {
    this.refreshReconfigurators();
    return this.CH_RC.getReplicatedServers(name);
  }

  // refresh before returning
  public NodeIDType getFirstReconfigurator(String name) {
    this.refreshReconfigurators();
    return this.CH_RC.getNode(name);
  }

  // NOT USED IN NEW APP. FOR BACKWARDS COMPATIBILITY WITH OLD APP.
  // WILL BE REMOVED AFTER NEW APP IS TESTED.
  /**
   * Returns the hash for this name.
   */
  @Deprecated
  public NodeIDType getReconfiguratorHash(String name) {
    return this.getFirstReconfigurator(name);
  }

  // refresh before returning
  public Set<NodeIDType> getReplicatedActives(String name) {
    this.refreshActives();
    return this.CH_AR.getReplicatedServers(name);
  }

  public ArrayList<InetAddress> getReplicatedActivesIPs(String name) {
    return this.getNodeIPs(this.getReplicatedActives(name));
  }

  /*
   * This method maps a set of addresses, newAddresses, to a set of nodes such
   * that there is maximal overlap with the specified set of nodes, oldNodes.
   * It is somewhat nontrivial only because there is a many-to-one mapping
   * from nodes to addresses, so a simple reverse lookup is not meaningful.
   */
  public Set<NodeIDType> getIPToNodeIDs(ArrayList<InetAddress> newAddresses,
          Set<NodeIDType> oldNodes) {
    Set<NodeIDType> newNodes = new HashSet<NodeIDType>(); // return value
    ArrayList<InetAddress> unassigned = new ArrayList<InetAddress>();
    for (InetAddress address : newAddresses) {
      unassigned.add(address);
    }
    // assign old nodes first if they match any new address
    for (NodeIDType oldNode : oldNodes) {
      InetAddress oldAddress = this.nodeConfig.getNodeAddress(oldNode);
      if (unassigned.contains(oldAddress)) {
        newNodes.add(oldNode);
        unassigned.remove(oldAddress);
      }
    }
    // assign any node to unassigned addresses
    for (NodeIDType node : this.nodeConfig.getNodeIDs()) {
      InetAddress address = this.nodeConfig.getNodeAddress(node);
      if (unassigned.contains(address)) {
        newNodes.add(node);
        unassigned.remove(address);
      }
    }
    return newNodes;
  }

  public InterfaceReconfigurableNodeConfig<NodeIDType> getUnderlyingNodeConfig() {
    return this.nodeConfig;
  }

  // refresh consistent hash structure if changed
  private synchronized boolean refreshActives() {
    Set<NodeIDType> curActives = this.nodeConfig.getActiveReplicas();
    if (curActives.equals(this.getLastActives())) {
      return false;
    }
    this.setLastActives(curActives);
    this.CH_AR.refresh(curActives);
    return true;
  }

  // refresh consistent hash structure if changed
  private synchronized boolean refreshReconfigurators() {
    Set<NodeIDType> curReconfigurators = this.nodeConfig
            .getReconfigurators();
    if (curReconfigurators.equals(this.getLastReconfigurators())) {
      return false;
    }
    this.setLastReconfigurators(curReconfigurators);
    this.CH_RC.refresh(curReconfigurators);
    return true;
  }

  private synchronized Set<NodeIDType> getLastActives() {
    return this.activeReplicas;
  }

  private synchronized Set<NodeIDType> getLastReconfigurators() {
    return this.reconfigurators;
  }

  private synchronized Set<NodeIDType> setLastActives(
          Set<NodeIDType> curActives) {
    return this.activeReplicas = curActives;
  }

  private synchronized Set<NodeIDType> setLastReconfigurators(
          Set<NodeIDType> curReconfigurators) {
    return this.reconfigurators = curReconfigurators;
  }

}
