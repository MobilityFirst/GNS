package edu.umass.cs.gns.nsdesign.nodeconfig;

import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;

import edu.umass.cs.gns.nsdesign.nodeconfig.InterfaceNodeConfig;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import java.net.InetAddress;
import java.util.Set;

/**
 * Implements node config interface that we will use for coordination among name servers.
 *
 * We created this class so that getNodeIDs() returns only list of name servers, so that the failure detector inside
 * replica-coordination does not try to detect failure of local name servers.
 *
 * Created by abhigyan on 3/30/14.
 */
public class NSNodeConfig implements InterfaceNodeConfig<NodeId<String>> {

  GNSNodeConfig gnsNodeConfig;

  public NSNodeConfig(GNSNodeConfig gnsNodeConfig) {
    this.gnsNodeConfig = gnsNodeConfig;
  }

  @Override
  public boolean nodeExists(NodeId<String> nodeId) {
    return getNodeIDs().contains(nodeId);
  }

  @Override
  public Set<NodeId<String>> getNodeIDs() {
    return gnsNodeConfig.getNodeIDs();
  }

  @Override
  public InetAddress getNodeAddress(NodeId<String> ID) {
    return gnsNodeConfig.getNodeAddress(ID);
  }

  @Override
  public int getNodePort(NodeId<String> ID) {
    return gnsNodeConfig.getNSTcpPort(ID);
  }

}
