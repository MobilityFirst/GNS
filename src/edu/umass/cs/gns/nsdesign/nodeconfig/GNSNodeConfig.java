package edu.umass.cs.gns.nsdesign.nodeconfig;

import com.google.common.collect.ImmutableSet;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.InterfaceNodeConfig;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.util.HostInfo;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class parses configuration files to gather information about each name server/local name
 * server in the system.
 *
 * Note that Local Name Server info is being flushed from this. LNSs no longer have IDs. Nobody needs to
 * know about them other than clients.
 *
 * To use the nio package, GNS implements <code>NodeConfig</code> interface in this class.
 *
 * @author Abhigyan
 *
 * Arun: FIXME: Unclear why we
 * have both NSNodeConfig and GNSNodeConfig. The former should be retrievable
 * from here.
 */
public class GNSNodeConfig implements InterfaceNodeConfig<Integer> {

  private int nodeID = -1; // this will be -1 for Local Name Servers

  public static final long INVALID_PING_LATENCY = -1L;

  public static final int INVALID_NAME_SERVER_ID = -1;

  /**
   * Contains information about each name server. <Key = HostID, Value = HostInfo>
   *
   */
  private final ConcurrentMap<Integer, HostInfo> hostInfoMapping
          = new ConcurrentHashMap<Integer, HostInfo>(16, 0.75f, 8);

  // Currently only used by GNSINstaller
  /**
   * Creates an empty GNSNodeConfig 
   */
  public GNSNodeConfig() {
    // this doesn't set numberOfNameServers
  }

  /**
   * Creates a GNSNodeConfig and initializes it from a name server host file.
   * This supports the new hosts.txt style format.
   *
   * @param hostsFile
   * @param nameServerID
   */
  public GNSNodeConfig(String hostsFile, int nameServerID) throws IOException {
    if (isOldStyleFile(hostsFile)) {
      throw new UnsupportedOperationException("THE USE OF OLD STYLE NODE INFO FILES IS NOT LONGER SUPPORTED. FIX THIS FILE: " + hostsFile);
      //initFromOldStyleFile(hostsFile, nameServerID);
    } else {
      initFromNSFile(hostsFile, nameServerID);
    }
  }

  

  /**
   * Creates a GNSNodeConfig and initializes for a local installation
   *
   * @param nsHosts - number of NameServers created
   * @param nameServerID - the id of this server
   */
  public GNSNodeConfig(int nsHosts, int nameServerID) {
    this.nodeID = nameServerID;
    initServersCount(0, nsHosts);
    GNS.getLogger().info("Number of name servers is : " + nsHosts);
  }
  
  /**
   * **
   * Parse a pair of ns host file to create a mapping of node information for name servers.
   *
   * @param nsHostsFile
   * @param nameServerID
   * @throws NumberFormatException
   */
  private void initFromNSFile(String nsHostsFile, int nameServerID) {
    this.nodeID = nameServerID;
    initServersFromFile(nsHostsFile);
  }

  private void initServersFromFile(String hostsFile) {
    List<HostFileLoader.HostSpec> hosts = null;
    try {
      hosts = HostFileLoader.loadHostFile(hostsFile);
    } catch (Exception e) {
      GNS.getLogger().severe("Problem loading hosts file file: " + e);
      e.printStackTrace();
      return;
    }
    for (HostFileLoader.HostSpec spec : hosts) {
      int id = spec.getId();
      String ipAddressString = spec.getName();
      addHostInfo(id, ipAddressString);
    }
  }

  private void initServersCount(int startId, int count) {
    try {
      String ipAddressString = InetAddress.getLocalHost().getHostAddress();
      for (int id = startId; id < count + startId; id++) {
        int port = GNS.NS_LOCAL_FIRST_NODE_PORT + id * 10;
        addHostInfo(id, ipAddressString, port, 0, 0, 0);
      }
    } catch (UnknownHostException e) {
      System.err.println("Problem getting local host address: " + e);
    }
  }

  /**
   * Adds a HostInfo object to the list maintained by this config instance.
   *
   * @param id
   * @param ipAddress
   * @param startingPort
   * @param pingLatency
   * @param latitude
   * @param longitude
   */
  public void addHostInfo(int id, String ipAddress, int startingPort, long pingLatency, double latitude, double longitude) {
    HostInfo nodeInfo = new HostInfo(id, ipAddress, startingPort, pingLatency, latitude, longitude);
    GNS.getLogger().fine(nodeInfo.toString());
    hostInfoMapping.put(id, nodeInfo);
  }

  /**
   * Adds a HostInfo object to the list maintained by this config instance.
   *
   * @param id
   * @param ipAddress
   */
  public void addHostInfo(int id, String ipAddress) {
    HostInfo nodeInfo = new HostInfo(id, ipAddress, GNS.STARTINGPORT, 0, 0, 0);
    GNS.getLogger().fine(nodeInfo.toString());
    hostInfoMapping.put(id, nodeInfo);
  }

  /**
   * Returns the complete set of IDs for all name servers (not local name servers).
   *
   * @return the set of IDs.
   */
  @Override
  public Set<Integer> getNodeIDs() {
    return ImmutableSet.copyOf(hostInfoMapping.keySet());
  }
  
  public int getNumberOfNodes() {
    return hostInfoMapping.size();
  }

  /**
   *
   * Returns the HostInfo structure for a host
   *
   * @param id
   * @return NameServerInfo *
   */
  public HostInfo getHostInfo(int id) {
    return hostInfoMapping.get(id);
  }

  /**
   * Returns the TCP port of a nameserver
   *
   * @param id Nameserver id
   * @return the stats port for a nameserver
   */
  public int getNSTcpPort(int id) {
    HostInfo nodeInfo = hostInfoMapping.get(id);
    return (nodeInfo == null) ? -1 : nodeInfo.getStartingPortNumber() + GNS.PortType.NS_TCP_PORT.getOffset();
  }

  public int getNSUdpPort(int id) {
    HostInfo nodeInfo = hostInfoMapping.get(id);
    return (nodeInfo == null) ? -1 : nodeInfo.getStartingPortNumber() + GNS.PortType.NS_UDP_PORT.getOffset();
  }

  /**
   * Returns the Admin port of a Nameserver
   *
   * @param id Nameserver id
   * @return the active nameserver information port of a nameserver. *
   */
  public int getNSAdminRequestPort(int id) {
    HostInfo nodeInfo = hostInfoMapping.get(id);
    return (nodeInfo == null) ? -1 : nodeInfo.getStartingPortNumber() + GNS.PortType.NS_ADMIN_PORT.getOffset();
  }

  /**
   * Returns the NS ping port
   *
   * @param id
   * @return the port
   */
  public int getNSPingPort(int id) {
    HostInfo nodeInfo = hostInfoMapping.get(id);
    return (nodeInfo == null) ? -1 : nodeInfo.getStartingPortNumber() + GNS.PortType.NS_PING_PORT.getOffset();
  }

  /**
   * Returns the IP address of a name server.
   *
   * @param id Server id
   * @return IP address of a server
   */
  @Override
  public InetAddress getNodeAddress(Integer id) {
    HostInfo nodeInfo = hostInfoMapping.get(id);
    return (nodeInfo == null) ? null : nodeInfo.getIpAddress();
  }

  /**
   * Returns the ping latency between two servers
   *
   * @param id Server id
   * @return
   */
  public long getPingLatency(int id) {
    HostInfo nodeInfo = hostInfoMapping.get(id);
    return (nodeInfo == null) ? -1 : nodeInfo.getPingLatency();
  }

  public void updatePingLatency(int id, long responseTime) {
    HostInfo nodeInfo = hostInfoMapping.get(id);
    if (nodeInfo != null) {
      nodeInfo.updatePingLatency(responseTime);
    }
  }

  @Override
  public boolean nodeExists(Integer ID) {
    return getNodeIDs().contains(ID);
  }

  @Override
  public int getNodePort(Integer ID) {
    return this.getNSTcpPort(ID);
  }

  /**
   * Returns the Name Server (not including Local Name Servers) with lowest latency.
   *
   * @return id of closest server or INVALID_NAME_SERVER_ID if one can't be found
   */
  public int getClosestServer() {
    return GNSNodeConfig.this.getClosestServer(getNodeIDs());
  }

  /**
   * Selects the closest Name Server from a set of Name Servers.
   *
   * @param servers
   * @return id of closest server or INVALID_NAME_SERVER_ID if one can't be found
   */
  public int getClosestServer(Set<Integer> servers) {
    return GNSNodeConfig.this.getClosestServer(servers, null);
  }

  /**
   * Selects the closest Name Server from a set of Name Servers.
   * excludeNameServers is a set of Name Servers from the first list to not consider.
   *
   * @param serverIds
   * @param excludeServers
   * @return id of closest server or INVALID_NAME_SERVER_ID if one can't be found
   */
  public int getClosestServer(Set<Integer> serverIds, Set<Integer> excludeServers) {
    if (serverIds == null) {
      return INVALID_NAME_SERVER_ID;
    }
    if (serverIds.contains(nodeID) && excludeServers != null && !excludeServers.contains(nodeID)) {
      return nodeID;
    }

    long lowestLatency = Long.MAX_VALUE;
    int nameServerID = INVALID_NAME_SERVER_ID;
    for (Integer serverId : serverIds) {
      if (excludeServers != null && excludeServers.contains(serverId)) {
        continue;
      }
      long pingLatency = getPingLatency(serverId);
      if (pingLatency >= 0 && pingLatency < lowestLatency) {
        lowestLatency = pingLatency;
        nameServerID = serverId;
      }
    }

    return nameServerID;
  }
  
  /**
   * Returns true if the file is the old style (has lots of fields).
   *
   * @param file
   * @return
   */
  private boolean isOldStyleFile(String file) throws IOException {
    try {
      BufferedReader reader = new BufferedReader(new FileReader(file));
      if (!reader.ready()) {
        throw new IOException("Problem reading host config file " + file);
      }
      String line = reader.readLine();
      if (line == null) {
        throw new IOException("Host config file is empty" + file);
      }
      return line.split("\\s+").length > 4;
    } catch (IOException e) {
      System.out.println("Problem reading host config file:" + e);
      return false;
    }
  }

  /**
   * Tests *
   */
  public static void main(String[] args) throws Exception {
    String filename = Config.WESTY_GNS_DIR_PATH + "/conf/name-server-info";
    GNSNodeConfig gnsNodeConfigOldSchool = new GNSNodeConfig(filename, 44);
    System.out.println(gnsNodeConfigOldSchool.hostInfoMapping.toString());
    System.out.println(gnsNodeConfigOldSchool.getNumberOfNodes());

    GNSNodeConfig gnsNodeConfig = new GNSNodeConfig(Config.WESTY_GNS_DIR_PATH + "/conf/ec2_release/ns_hosts.txt", 44);
    System.out.println("hostInfoMapping:" + gnsNodeConfig.hostInfoMapping.toString());
    System.out.println(gnsNodeConfig.getNumberOfNodes());
  }

}
