package edu.umass.cs.gns.nsdesign;

import com.google.common.collect.ImmutableSet;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.NodeConfig;
import edu.umass.cs.gns.util.HostInfo;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class parses configuration files to gather information about each name server/local name
 * server in the system.
 *
 * To use the nio package, GNS implements <code>NodeConfig</code> interface in this class.
 *
 * Earlier implementation of this class was static. We have made this class non-static to be able to test
 * with multiple nodes within a single JVM.
 *
 *
 * @author Abhigyan
 */
public class GNSNodeConfig implements NodeConfig {

  private int nodeID = -1;

  public static final long INVALID_PING_LATENCY = -1L;
  
  public static final int INVALID_NAME_SERVER_ID = -1;

  /**
   * Contains information about each name server. <Key = HostID, Value = HostInfo>
   *
   */
  private ConcurrentMap<Integer, HostInfo> hostInfoMapping =
          new ConcurrentHashMap<Integer, HostInfo>(16, 0.75f, 8);
  /**
   * A subset of hostInfoMapping with just the ids of the nameservers, not the LNSs
   */
  private ConcurrentMap<Integer, Integer> nameServerMapping =
          new ConcurrentHashMap<Integer, Integer>(16, 0.75f, 8);
  /**
   * A subset hostInfoMapping with just the ids of the Local Name Server, not the NSs
   */
  private ConcurrentMap<Integer, Integer> localNameServerMapping =
          new ConcurrentHashMap<Integer, Integer>(16, 0.75f, 8);


  private Random rand = new Random();

  /**
   * Creates an empty GNSNodeConfig
   */
  public GNSNodeConfig() {
    // this doesn't set numberOfNameServers
  }

  /**
   * Creates a GNSNodeConfig and initializes it from a file
   * 
   * @param nodeInfoFile
   * @param nameServerID 
   */
  public GNSNodeConfig(String nodeInfoFile, int nameServerID) {
    initFromFile(nodeInfoFile, nameServerID);
  }
  
  /**
   * **
   * Parse the host's information file to create a mapping of node information for name servers and local name severs
   *
   * @param nodeInfoFile Format: HostID IsNS? IPAddress StartingPort Ping-Latency Latitude Longitude
   * @param nameServerID
   * @throws NumberFormatException
   */
  public final void initFromFile(String nodeInfoFile, int nameServerID) {
    this.nodeID = nameServerID;
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
          localNameServerMapping.put(id, id);
        }
        InetAddress ipAddress = null;
        try {
          ipAddress = InetAddress.getByName(ipAddressString);
        } catch (UnknownHostException e) {
          System.err.println("Problem parsing IP address for NS " + nameServerID + " :" + e);
        }
        int startingPort;
        if (startingPortString.startsWith("-") || startingPortString.startsWith("default")) {
          startingPort = GNS.STARTINGPORT;
        } else {
          startingPort = Integer.parseInt(startingPortString);
        }
        addHostInfo(id, ipAddress, startingPort, pingLatency, latitude, longitude);
      }
      br.close();
    } catch (NumberFormatException e) {
      System.err.println("Problem parsing number in host config for NS " + nameServerID + " :" + e);
    } catch (IOException e) {
      System.err.println("Problem reading host config for NS " + nameServerID + " :" + e);
    }
    GNS.getLogger().fine("Number of name servers is : " + nameServerCount);
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
  public void addHostInfo(int id, InetAddress ipAddress, int startingPort, long pingLatency, double latitude, double longitude) {
    HostInfo nodeInfo = new HostInfo(id, ipAddress, startingPort, pingLatency, latitude, longitude);
    GNS.getLogger().fine(nodeInfo.toString());
    hostInfoMapping.put(id, nodeInfo);
  }
  

  /**
   * Returns the complete set of IDs for all servers (local and otherwise).
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
   * Returns the complete set of IDs for all local name servers.
   * 
   * @return The set of IDs.
   */
  public Set<Integer> getLocalNameServerIDs() {
    return ImmutableSet.copyOf(localNameServerMapping.keySet());
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

  /**
   * Returns the TCP port of a Local Nameserver
   *
   * @param id Nameserver id
   * @return the stats port for a nameserver
   */
  public int getLNSTcpPort(int id) {
    HostInfo nodeInfo = hostInfoMapping.get(id);
    return (nodeInfo == null) ? -1 : nodeInfo.getStartingPortNumber() + GNS.PortType.LNS_TCP_PORT.getOffset();
  }

  /**
   * Returns the UDP port of a Local Nameserver
   *
   * @param id
   * @return
   */
  public int getLNSUdpPort(int id) {
    HostInfo nodeInfo = hostInfoMapping.get(id);
    return (nodeInfo == null) ? -1 : nodeInfo.getStartingPortNumber() + GNS.PortType.LNS_UDP_PORT.getOffset();
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
   * Returns the Admin port of a Local Nameserver
   *
   * @param id
   * @return the port
   */
  public int getLNSAdminRequestPort(int id) {
    HostInfo nodeInfo = hostInfoMapping.get(id);
    return (nodeInfo == null) ? -1 : nodeInfo.getStartingPortNumber() + GNS.PortType.LNS_ADMIN_PORT.getOffset();
  }

  /**
   * Returns the response port of a Local nameserver
   *
   * @param id
   * @return the port
   */
  public int getLNSAdminResponsePort(int id) {
    HostInfo nodeInfo = hostInfoMapping.get(id);
    return (nodeInfo == null) ? -1 : nodeInfo.getStartingPortNumber() + GNS.PortType.LNS_ADMIN_RESPONSE_PORT.getOffset();
  }

  /**
   * Returns the dump response port of a Local nameserver
   *
   * @param id
   * @return the port
   */
  public int getLNSAdminDumpReponsePort(int id) {
    HostInfo nodeInfo = hostInfoMapping.get(id);
    return (nodeInfo == null) ? -1 : nodeInfo.getStartingPortNumber() + GNS.PortType.LNS_ADMIN_DUMP_RESPONSE_PORT.getOffset();
  }

  /**
   * Returns the LNS ping port
   * 
   * @param id
   * @return the port
   */
  public int getLNSPingPort(int id) {
    HostInfo nodeInfo = hostInfoMapping.get(id);
    return (nodeInfo == null) ? -1 : nodeInfo.getStartingPortNumber() + GNS.PortType.LNS_PING_PORT.getOffset();
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
      return getNSPingPort(id);
    }
  }

  /**
   * Returns the IP address of a name server.
   *
   * @param id Server id
   * @return IP address of a server
   */
  @Override
  public InetAddress getNodeAddress(int id) {
    HostInfo nodeInfo = hostInfoMapping.get(id);
    return (nodeInfo == null) ? null : nodeInfo.getIpAddress();
  }

  /**
   * Returns the ping latency between two servers
   *
   * @param id Server id
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
  public boolean containsNodeInfo(int ID) {
    return getNodeIDs().contains(ID);
  }

  @Override
  public int getNodePort(int ID) {
    if (this.isNameServer(ID)) {
      return this.getNSTcpPort(ID);
    }
    return this.getLNSTcpPort(ID);
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
   * Returns the Local Name Server (not including Name Servers) with lowest latency.
   * 
   * @return id of closest server or INVALID_NAME_SERVER_ID if one can't be found
   */
  public int getClosestLocalNameServer() {
    return getClosestServer(getLocalNameServerIDs());
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

//    // if multiple name servers are nearly equidistant, select randomly among them for load-balancing
//    if (nameServerID != INVALID_NAME_SERVER_ID) {
//      ArrayList<Integer> closeNameServers = new ArrayList<Integer>();
//      long tolerance = 5; // tolerate few extra ms delay
//      for (Integer serverId: serverIds) {
//        if (excludeServers != null && excludeServers.contains(serverId)) {
//          continue;
//        }
//        long pingLatency = getPingLatency(serverId);
//        if (pingLatency >= 0 && pingLatency < lowestLatency + tolerance) {
//          closeNameServers.add(serverId);
//        }
//      }
//
//      nameServerID = closeNameServers.get(rand.nextInt(closeNameServers.size()));
//    }
    return nameServerID;
  }

  /**
   * Tests *
   */
  public void main(String[] args) throws Exception {
    GNSNodeConfig GNSNodeConfig = new GNSNodeConfig("name-server-info", 44);
    System.out.println(GNSNodeConfig.hostInfoMapping.toString());
    System.out.println(GNSNodeConfig.getNameServerIDs().size());
//    System.out.println(GNSNodeConfig.getClosestNameServer() + "\t" + getPingLatency(getClosestNameServer()));

    Set<Integer> nameservers = new HashSet<Integer>();
    nameservers.add(1);
    nameservers.add(45);
    nameservers.add(48);
    nameservers.add(36);
    nameservers.add(59);
    Set<Integer> nameserverQueried = new HashSet<Integer>();
    nameserverQueried.add(8);
    System.out.println(GNSNodeConfig.this.getClosestServer(nameservers, null));
  }



}