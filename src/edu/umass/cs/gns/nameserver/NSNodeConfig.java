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
public class NSNodeConfig implements NodeConfig {

  @Override
  public boolean containsNodeInfo(int ID) {
    return ConfigFileInfo.isNameServer(ID);
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
    return ConfigFileInfo.getStatsPort(ID);
  }
}
