package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.nio.NodeConfig;
import edu.umass.cs.gns.util.ConfigFileInfo;

import java.net.InetAddress;

/**
 * To use the nio package, GNS implements <code>NodeConfig</code>. interface
 * in this class.
 *
 * It calls methods in <code>ConfigFileInfo</code> to implements all the methods.
 * This hardly contains any additional code.
 *
 * User: abhigyan
 * Date: 8/16/13
 * Time: 12:10 PM
 * @deprecated
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
    return ConfigFileInfo.getIPAddress(ID);
  }

  @Override
  public int getNodePort(int ID) {
    if (ConfigFileInfo.isNameServer(ID)) {
      return ConfigFileInfo.getNSTcpPort(ID);
    } else {
      return ConfigFileInfo.getLNSTcpPort(ID);
    }
  }
  
}
