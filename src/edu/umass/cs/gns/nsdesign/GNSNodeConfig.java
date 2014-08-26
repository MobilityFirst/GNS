package edu.umass.cs.gns.nsdesign;

import com.google.common.collect.ImmutableSet;
import edu.umass.cs.gns.installer.HostFileLoader;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.InterfaceNodeConfig;
import edu.umass.cs.gns.util.HostInfo;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
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
  /**
   * A subset of hostInfoMapping with just the ids of the nameservers, not the LNSs
   */
  private final ConcurrentMap<Integer, Integer> nameServerMapping
          = new ConcurrentHashMap<Integer, Integer>(16, 0.75f, 8);

//  /**
//   * A subset hostInfoMapping with just the ids of the Local Name Server, not the NSs
//   */
//  private final ConcurrentMap<Integer, Integer> localNameServerMapping
//          = new ConcurrentHashMap<Integer, Integer>(16, 0.75f, 8);
  /**
   * Creates an empty GNSNodeConfig
   */
  public GNSNodeConfig() {
    // this doesn't set numberOfNameServers
  }

  /**
   * Creates a GNSNodeConfig and initializes it from an old style name-server-info file.
   *
   * @param nodeInfoFile
   * @param nameServerID
   * @return
   */
  @Deprecated
  public static GNSNodeConfig CreateGNSNodeConfigFromOldStyleFile(String nodeInfoFile, int nameServerID) {
    return new GNSNodeConfig(nodeInfoFile, nameServerID, true);
  }

  /**
   * Creates a GNSNodeConfig and initializes it from a name-server-info file.
   * This is the legacy format and will go away.
   *
   * @param nodeInfoFile
   * @param nameServerID
   */
  @Deprecated
  private GNSNodeConfig(String nodeInfoFile, int nameServerID, boolean old) {
    initFromOldStyleFile(nodeInfoFile, nameServerID);
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
      initFromOldStyleFile(hostsFile, nameServerID);
    } else {
      initFromNSFile(hostsFile, nameServerID);
    }
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
   * Creates a GNSNodeConfig and initializes for a local installation
   *
   * @param nsHosts - number of NameServers created
   * @param nameServerID - the id of this server
   */
  public GNSNodeConfig(int nsHosts, int nameServerID) {
    this.nodeID = nameServerID;
    initServersCount(0, nsHosts, nameServerMapping);
    GNS.getLogger().info("Number of name servers is : " + nsHosts);
  }

  /**
   * **
   * Parse the host's information file to create a mapping of node information for name servers.
   * LOCAL NAME SERVER INFO IN THESE FILES IS NOW IGNORED.
   *
   * @param nodeInfoFile Format: HostID IsNS? IPAddress StartingPort Ping-Latency Latitude Longitude
   * @param nameServerID
   * @throws NumberFormatException
   */
  private void initFromOldStyleFile(String nodeInfoFile, int nameServerID) {
    this.nodeID = nameServerID;
    long t0 = System.currentTimeMillis();
    // Reads in data from a text file containing information about each name server
    // in the system.
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(nodeInfoFile));
    } catch (FileNotFoundException e1) {
      GNS.getLogger().severe("Host info file not found: " + e1);
      System.exit(0);
    }

    int nameServerCount = 0;

    try {
      while (br.ready()) {
        String line = br.readLine();
        if (line == null || line.equals("") || line.equals(" ")) {
          continue;
        }

        String[] tokens = line.split("\\s+");

        // Check for file's format
        if (tokens.length != 7) {
          System.err.println("Error: File " + nodeInfoFile + " formatted incorrectly");
          System.err.println("HostId: " + nameServerID);
          System.err.println("Token Length: " + tokens.length + " ");
          for (String str : tokens) {
            System.err.println(str);
          }
          System.err.println("Format:\nHostID IsNS? IPAddress [StartingPort | - ] Ping-Latency(ms) Latitude Longitude");
          System.err.println("\nwhere IsNS? is yes or no\n and StartingPort is a port number, but can also be a dash \"-\""
                  + " (or the word \"default\") to indicate that the default port can be used in the distributed case.");
          //System.err.println("Format:\nNameServerID IPAddress Ping-Latency Latitude Longitude");
          System.exit(0);
        }

        int id = Integer.parseInt(tokens[0]);
        String isNameServerString = tokens[1];
        String ipAddressString = tokens[2];
        String startingPortString = tokens[3];
        long pingLatency = Long.parseLong(tokens[4]);
        double latitude = Double.parseDouble(tokens[5]);
        double longitude = Double.parseDouble(tokens[6]);

        if (isNameServerString.startsWith("yes")
                || isNameServerString.startsWith("Yes")
                || isNameServerString.startsWith("YES")
                || isNameServerString.startsWith("X")
                || isNameServerString.startsWith("true")
                || isNameServerString.startsWith("True")
                || isNameServerString.startsWith("TRUE")) {
          nameServerMapping.put(id, id);
          nameServerCount++;
        } else {
          //localNameServerMapping.put(id, id);
        }

        int startingPort;
        if (startingPortString.startsWith("-") || startingPortString.startsWith("default")) {
          startingPort = GNS.STARTINGPORT;
        } else {
          startingPort = Integer.parseInt(startingPortString);
        }
        if (ipAddressString.startsWith("-")
                || ipAddressString.startsWith("local")) {
          ipAddressString = InetAddress.getLocalHost().getHostAddress();
        }
        addHostInfo(id, ipAddressString, startingPort, pingLatency, latitude, longitude);
      }
      br.close();
    } catch (NumberFormatException e) {
      System.err.println("Problem parsing number in host config for NS " + nameServerID + " :" + e);
    } catch (IOException e) {
      System.err.println("Problem reading host config for NS " + nameServerID + " :" + e);
    }
    GNS.getLogger().fine("Number of name servers is : " + nameServerCount);
    long t1 = System.currentTimeMillis();
    GNS.getStatLogger().info("Time to read all hosts info: " + (t1 - t0) / 1000 + " sec");
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
    int numberOfNameServers = initServersFromFile(0, nsHostsFile, nameServerMapping);
    GNS.getLogger().info("Number of name servers is : " + numberOfNameServers);
  }

  private int initServersFromFile(int startId, String hostsFile, ConcurrentMap<Integer, Integer> serverMap) {
    List<String> hosts = null;
    try {
      hosts = HostFileLoader.loadHostFile(hostsFile);
    } catch (FileNotFoundException e) {
      GNS.getLogger().severe("Hosts file not found: " + e);
      return -1;
    }
    int id = startId;
    for (String ipAddressString : hosts) {
      addHostInfo(id, ipAddressString);
      serverMap.put(id, id);
      id = id + 1;
    }
    return id;
  }

  private void initServersCount(int startId, int count, ConcurrentMap<Integer, Integer> serverMap) {
    try {
      String ipAddressString = InetAddress.getLocalHost().getHostAddress();
      for (int id = startId; id < count + startId; id++) {
        int port = GNS.NS_LOCAL_FIRST_NODE_PORT + id * 10;
        addHostInfo(id, ipAddressString, port, 0, 0, 0);
        serverMap.put(id, id);
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
   * Returns the complete set of IDs for all servers (local and otherwise).
   * THIS IS THE SAME AS getNameServerIDs NOW THAT LNSs DO NOT HAVE IDS.
   * FIXME: Check uses of this an fix as necessary then maybe delete one or the other.
   *
   * @return
   */
  @Override
  public Set<Integer> getNodeIDs() {
    return ImmutableSet.copyOf(hostInfoMapping.keySet());
  }

  /**
   * Returns the complete set of IDs for all name servers (not local name servers).
   *
   * @return The set of IDs.
   */
  public Set<Integer> getNameServerIDs() {
    return ImmutableSet.copyOf(nameServerMapping.keySet());
  }

  /**
   * Returns true if this is a name server (as opposed to a local name server).
   *
   * @param id
   * @return
   */
  public boolean isNameServer(int id) {
    if (nameServerMapping.containsKey(id)) {
      return true;
    }
    return false;
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
   * Returns the appropriate ping port for a server.
   *
   * @param id
   * @return the port
   */
  public int getPingPort(int id) {
    if (isNameServer(id)) {
      return getNSPingPort(id);
    } else {
      return GNS.DEFAULT_LNS_PING_PORT;
    }
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
  public boolean containsNodeInfo(Integer ID) {
    return getNodeIDs().contains(ID);
  }

  @Override
  public int getNodePort(Integer ID) {
    if (this.isNameServer(ID)) {
      return this.getNSTcpPort(ID);
    }
    return GNS.DEFAULT_LNS_TCP_PORT;
  }

  /**
   * Returns the Name Server (not including Local Name Servers) with lowest latency.
   *
   * @return id of closest server or INVALID_NAME_SERVER_ID if one can't be found
   */
  public int getClosestNameServer() {
    return getClosestServer(getNameServerIDs());
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
   * Tests *
   */
  public static void main(String[] args) throws Exception {
    String filename = Config.WESTY_GNS_DIR_PATH + "/conf/name-server-info";
    GNSNodeConfig gnsNodeConfigOldSchool = GNSNodeConfig.CreateGNSNodeConfigFromOldStyleFile(filename, 44);
    System.out.println(gnsNodeConfigOldSchool.hostInfoMapping.toString());
    System.out.println(gnsNodeConfigOldSchool.getNameServerIDs().size());

    GNSNodeConfig gnsNodeConfig = new GNSNodeConfig(Config.WESTY_GNS_DIR_PATH + "/conf/ec2_release/ns_hosts.txt", 44);
    System.out.println("hostInfoMapping:" + gnsNodeConfig.hostInfoMapping.toString());
    System.out.println(gnsNodeConfig.getNameServerIDs().size());

    GNSNodeConfig gnsNodeConfigCount = new GNSNodeConfig(3, 0);
    System.out.println("hostInfoMapping:" + gnsNodeConfigCount.hostInfoMapping.toString());
  }

}
