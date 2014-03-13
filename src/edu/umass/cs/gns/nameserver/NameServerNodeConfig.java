package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.nio.NodeConfig;
import edu.umass.cs.gns.util.ConfigFileInfo;

import java.net.InetAddress;

/**
 * Implements node config interface that we will use for paxos manager. It only returns information
 * about name servers and not local name servers, because local name servers are not involved in paxos.
 *
 * Created by abhigyan on 3/13/14.
 */
public class NameServerNodeConfig implements NodeConfig {

  @Override
  public boolean containsNodeInfo(int ID) {
    return ID < ConfigFileInfo.getNumberOfNameServers();
  }

  @Override
  public int getNodeCount() {
    return ConfigFileInfo.getNumberOfNameServers();
  }

  @Override
  public InetAddress getNodeAddress(int ID) {
    return ConfigFileInfo.getIPAddress(ID);
  }

  @Override
  public int getNodePort(int ID) {
    if (ConfigFileInfo.isNameServer(ID)) {
      return ConfigFileInfo.getNSTcpPort(ID);
    } else {
      return -1;
    }
  }

}
