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
    this.CH = new ConsistentHashing<InetSocketAddress>(this.nodes);
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

  @Override
  public boolean nodeExists(InetSocketAddress id) {
    return this.nodeConfig.nodeExists(id);
  }

  @Override
  public InetAddress getNodeAddress(InetSocketAddress id) {
    return this.nodeConfig.getNodeAddress(id);
  }

  @Override
  public InetAddress getBindAddress(InetSocketAddress id) {
    return this.nodeConfig.getBindAddress(id);
  }

  @Override
  public int getNodePort(InetSocketAddress id) {
    return this.nodeConfig.getNodePort(id);
  }

  @Override
  public Set<InetSocketAddress> getNodeIDs() {
    throw new RuntimeException("The use of this method is not permitted");
    //return this.nodeConfig.getNodeIDs();
  }

  @Override
  public InetSocketAddress valueOf(String strValue) {
    return this.nodeConfig.valueOf(strValue);
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
}
