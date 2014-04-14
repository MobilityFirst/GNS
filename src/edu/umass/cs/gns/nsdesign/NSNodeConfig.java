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
  public boolean containsNodeInfo(int ID) {
    return ID < gnsNodeConfig.getNumberOfNameServers();
  }

  @Override
  public int getNodeCount() {
    return gnsNodeConfig.getNumberOfNameServers();
  }

  @Override
  public Set<Integer> getNodeIDs() {
    return gnsNodeConfig.getAllNameServerIDs();
  }

  @Override
  public InetAddress getNodeAddress(int ID) {
    return gnsNodeConfig.getIPAddress(ID);
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

