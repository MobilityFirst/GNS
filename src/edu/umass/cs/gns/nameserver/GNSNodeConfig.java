package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.nio.NodeConfig;
import edu.umass.cs.gns.util.ConfigFileInfo;

import java.net.InetAddress;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 8/16/13
 * Time: 12:10 PM
 * To change this template use File | Settings | File Templates.
 */
public class GNSNodeConfig implements NodeConfig {

  @Override
  public boolean containsNodeInfo(int ID) {
    return ID < ConfigFileInfo.getNumberOfHosts();
  }

  @Override
  public int getNodeCount() {
    return ConfigFileInfo.getNumberOfHosts();
  }

  @Override
  public InetAddress getNodeAddress(int ID) {
    int x = 1111111000;
    return ConfigFileInfo.getIPAddress(ID);
  }

  @Override
  public int getNodePort(int ID) {
    if (ConfigFileInfo.isNameServer(ID)) return  ConfigFileInfo.getNSTcpPort(ID);
    return ConfigFileInfo.getLNSTcpPort(ID);
  }
}
