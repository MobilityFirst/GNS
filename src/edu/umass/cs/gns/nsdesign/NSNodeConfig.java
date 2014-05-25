package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.nio.NodeConfig;

import java.net.InetAddress;
import java.util.Set;

/**
 * Implements node config interface that we will use for coordination among name servers. It only returns information
 * about name servers and not local name servers.
 *
 * Created by abhigyan on 3/30/14.
 */
public class NSNodeConfig implements NodeConfig {

  GNSNodeConfig gnsNodeConfig;

  public NSNodeConfig(GNSNodeConfig gnsNodeConfig) {
    this.gnsNodeConfig = gnsNodeConfig;
  }
  @Override
  public boolean containsNodeInfo(int nodeId) {
    return gnsNodeConfig.getNameServerIDs().contains(nodeId);
  }


  @Override
  public Set<Integer> getNodeIDs() {
    return gnsNodeConfig.getNameServerIDs();
  }

  @Override
  public InetAddress getNodeAddress(int ID) {
    return gnsNodeConfig.getNodeAddress(ID);
  }

  @Override
  public int getNodePort(int ID) {
    if (gnsNodeConfig.isNameServer(ID)) {
      return gnsNodeConfig.getNSTcpPort(ID);
    } else {
      return -1;
    }
  }

}

