package edu.umass.cs.gns.nsdesign.nodeconfig;

import java.net.InetAddress;
import java.util.Set;

import edu.umass.cs.gns.nio.InterfaceNodeConfig;

/**
 * Implements node config interface that we will use for coordination among name servers.
 *
 * We created this class so that getNodeIDs() returns only list of name servers, so that the failure detector inside
 * replica-coordination does not try to detect failure of local name servers.
 *
 * Created by abhigyan on 3/30/14.
 * @param <NodeIDType>
 */
public class NSNodeConfig<NodeIDType> implements InterfaceNodeConfig<NodeIDType> {

  GNSNodeConfig gnsNodeConfig;

  public NSNodeConfig(GNSNodeConfig gnsNodeConfig) {
    this.gnsNodeConfig = gnsNodeConfig;
  }

  @Override
  public boolean nodeExists(NodeIDType nodeId) {
    return getNodeIDs().contains(nodeId);
  }

  @Override
  public Set<NodeIDType> getNodeIDs() {
    return gnsNodeConfig.getNodeIDs();
  }

  @Override
  public InetAddress getNodeAddress(NodeIDType ID) {
    return gnsNodeConfig.getNodeAddress(ID);
  }

  @Override
  public int getNodePort(NodeIDType ID) {
    return gnsNodeConfig.getNSTcpPort(ID);
  }

@Override
public NodeIDType valueOf(String nodeAsString) {
	throw new RuntimeException("Method not yet implemented");
}

}
