/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 */
package edu.umass.cs.gns.nsdesign.nodeconfig;

import com.google.common.collect.ImmutableSet;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.InterfaceNodeConfig;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.Shutdownable;
import edu.umass.cs.gns.util.ConsistentHashing;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class parses a hosts file to gather information about each name server (not local) server in the system.
 *
 * Also has support for checking to see if the hosts file changes. When that happens the host info is
 * reloaded and the <code>ConsistentHashing.reInitialize</code> method is called.
 *
 *
 * @param <NodeIDType>
 */
public class GNSNodeConfig<NodeIDType> implements InterfaceNodeConfig<NodeIDType>, Shutdownable {

  public static final long INVALID_PING_LATENCY = -1L;
  public static final int INVALID_PORT = -1;

  private NodeIDType nodeID; // if this is null you should check isLocalNameServer; otherwise it might be invalid
  private boolean isLocalNameServer = false;
  private final String hostsFile;

  /**
   * Contains information about each name server. <Key = HostID, Value = HostInfo>
   *
   */
  private ConcurrentMap<NodeIDType, HostInfo> hostInfoMapping;
  // keep this around
  private ConcurrentMap<NodeIDType, HostInfo> previousHostInfoMapping;

  public GNSNodeConfig(String hostsFile, boolean isLocalNameServer) throws IOException {
    this(hostsFile, null);
  }

  /**
   * Creates a GNSNodeConfig and initializes it from a name server host file.
   * This supports the new hosts.txt style format.
   *
   * @param hostsFile
   * @param nameServerID - specify null to mean the local name server
   */
  public GNSNodeConfig(String hostsFile, NodeIDType nameServerID) throws IOException {
    if (nameServerID == null) {
      this.isLocalNameServer = true;
    }
    this.nodeID = nameServerID;

    this.hostsFile = hostsFile;
    if (isOldStyleFile(hostsFile)) {
      throw new UnsupportedOperationException("THE USE OF OLD STYLE NODE INFO FILES IS NOT LONGER SUPPORTED. FIX THIS FILE: " + hostsFile);
      //initFromOldStyleFile(hostsFile, nameServerID);
    }
    readHostsFile(hostsFile);
    // Informational purposes
    for (Entry<NodeIDType, HostInfo> hostInfoEntry : hostInfoMapping.entrySet()) {
      GNS.getLogger().info("Id: " + hostInfoEntry.getValue().getId().toString()
              + " Host:" + hostInfoEntry.getValue().getIpAddress()
              + " Start Port:" + hostInfoEntry.getValue().getStartingPortNumber());
    }
    startCheckingForUpdates();
  }

  // Currently only used by GNSINstaller
  /**
   * Creates an empty GNSNodeConfig
   */
  public GNSNodeConfig() {
    this.nodeID = null;
    hostsFile = null;
  }

  /**
   * Returns the complete set of IDs for all name servers (not local name servers).
   *
   * @return the set of IDs.
   */
  @Override
  public Set<NodeIDType> getNodeIDs() {
    return ImmutableSet.copyOf(hostInfoMapping.keySet());
  }

  /**
   * Returns the complete set of IDs that existed before the latest change.
   *
   * @return
   */
  public Set<NodeIDType> getPreviousNodeIDs() {
    return ImmutableSet.copyOf(previousHostInfoMapping.keySet());
  }

  /**
   * Returns the number of name server nodes.
   *
   * @return the number of nodes
   */
  public int getNumberOfNodes() {
    return hostInfoMapping.size();
  }

  /**
   * Returns the TCP port of a nameserver.
   * Will return INVALID_NAME_SERVER_ID if the node doesn't exist.
   *
   * @param id Nameserver id
   * @return the stats port for a nameserver
   */
  public int getNSTcpPort(NodeIDType id) {
    HostInfo nodeInfo = hostInfoMapping.get(id);
    if (nodeInfo != null) {
      return nodeInfo.getStartingPortNumber() + GNS.PortType.NS_TCP_PORT.getOffset();
    } else {
      GNS.getLogger().warning("NodeId " + id.toString() + " not a valid Id!");
      return INVALID_PORT;
    }
  }

  /**
   * Returns the UDP port of a nameserver.
   * Will return INVALID_NAME_SERVER_ID if the node doesn't exist.
   *
   * @param id
   * @return
   */
  public int getNSUdpPort(NodeIDType id) {
    HostInfo nodeInfo = hostInfoMapping.get(id);
    return (nodeInfo == null) ? INVALID_PORT : nodeInfo.getStartingPortNumber() + GNS.PortType.NS_UDP_PORT.getOffset();
  }

  /**
   * Returns the Admin port of a Nameserver.
   * Will return INVALID_NAME_SERVER_ID if the node doesn't exist.
   *
   * @param id Nameserver id
   * @return the active nameserver information port of a nameserver. *
   */
  public int getNSAdminRequestPort(NodeIDType id) {
    HostInfo nodeInfo = hostInfoMapping.get(id);
    return (nodeInfo == null) ? INVALID_PORT : nodeInfo.getStartingPortNumber() + GNS.PortType.NS_ADMIN_PORT.getOffset();
  }

  /**
   * Returns the NS ping port.
   * Will return INVALID_NAME_SERVER_ID if the node doesn't exist.
   *
   * @param id
   * @return the port
   */
  public int getNSPingPort(NodeIDType id) {
    HostInfo nodeInfo = hostInfoMapping.get(id);
    if (nodeInfo != null) {
      return nodeInfo.getStartingPortNumber() + GNS.PortType.NS_PING_PORT.getOffset();
    } else {
      return INVALID_PORT;
    }
  }

  /**
   * Returns the IP address of a name server.
   * Will return null if the node doesn't exist.
   *
   * @param id Server id
   * @return IP address of a server
   */
  @Override
  public InetAddress getNodeAddress(NodeIDType id) {
    // handle special case for LNS node
    if (id instanceof InetSocketAddress) {
      return ((InetSocketAddress) id).getAddress();
    }
    HostInfo nodeInfo = hostInfoMapping.get(id);
    return (nodeInfo == null) ? null : nodeInfo.getIpAddress();
  }

  /**
   * Returns the ping latency between two servers.
   * Will return INVALID_PING_LATENCY if the node doesn't exist.
   *
   * @param id Server id
   * @return
   */
  public long getPingLatency(NodeIDType id) {
    HostInfo nodeInfo = hostInfoMapping.get(id);
    return (nodeInfo == null) ? INVALID_PING_LATENCY : nodeInfo.getPingLatency();
  }

  public void updatePingLatency(NodeIDType id, long responseTime) {
    HostInfo nodeInfo = hostInfoMapping.get(id);
    if (nodeInfo != null) {
      nodeInfo.updatePingLatency(responseTime);
    }
  }

  /**
   * Returns true if the node exists.
   *
   * @param ID
   * @return
   */
  @Override
  public boolean nodeExists(NodeIDType ID) {
    return getNodeIDs().contains(ID);
  }

  /**
   * Returns the TCP port of a nameserver.
   * Will return INVALID_NAME_SERVER_ID if the node doesn't exist.
   *
   * @param id
   * @return
   */
  @Override
  public int getNodePort(NodeIDType id) {
    // handle special case for LNS node
    if (id instanceof InetSocketAddress) {
      return ((InetSocketAddress) id).getPort();
    }
    return this.getNSTcpPort(id);
  }

  /**
   * **
   * Returns port number for the specified port type. Return -1 if the specified port type does not exist.
   *
   * @param nameServerId Name server id //
   * @param portType	GNS port type
   *
   * @return the port
   */
  public int getPort(NodeIDType nameServerId, GNS.PortType portType) {
    switch (portType) {
      case NS_TCP_PORT:
        return getNSTcpPort(nameServerId);
      case NS_UDP_PORT:
        return getNSUdpPort(nameServerId);
      case NS_ADMIN_PORT:
        return getNSAdminRequestPort(nameServerId);
      case NS_PING_PORT:
        return getNSPingPort(nameServerId);
    }
    return -1;
  }

  /**
   * Returns the Name Server (not including Local Name Servers) with lowest latency.
   *
   * @return id of closest server or INVALID_NAME_SERVER_ID if one can't be found
   */
  public NodeIDType getClosestServer() {
    return GNSNodeConfig.this.getClosestServer(getNodeIDs());
  }

  /**
   * Selects the closest Name Server from a set of Name Servers.
   *
   * @param servers
   * @return id of closest server or INVALID_NAME_SERVER_ID if one can't be found
   */
  public NodeIDType getClosestServer(Set<NodeIDType> servers) {
    return getClosestServer(servers, null);
  }

  /**
   * Selects the closest Name Server from a set of Name Servers.
   * excludeNameServers is a set of Name Servers from the first list to not consider.
   * If the local server is one of the serverIds and not excluded this will return it.
   *
   * @param serverIds
   * @param excludeServers
   * @return id of closest server or null if one can't be found
   */
  public NodeIDType getClosestServer(Set<NodeIDType> serverIds, Set<NodeIDType> excludeServers) {
    if (serverIds == null) {
      return null;
    }
    // If the local server is one of the server ids and not excluded return it.
    if (serverIds.contains(nodeID) && excludeServers != null && !excludeServers.contains(nodeID)) {
      return nodeID;
    }

    long lowestLatency = Long.MAX_VALUE;
    NodeIDType nameServerID = null;
    for (NodeIDType serverId : serverIds) {
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
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    String filename = Config.WESTY_GNS_DIR_PATH + "/conf/name-server-info";
    GNSNodeConfig gnsNodeConfig = new GNSNodeConfig(filename, 1);
    System.out.println(gnsNodeConfig.hostInfoMapping.toString());
    System.out.println(gnsNodeConfig.getNumberOfNodes());
    System.out.println(gnsNodeConfig.getNSTcpPort(2));
    System.out.println(gnsNodeConfig.getNodeAddress(0));
    System.out.println(gnsNodeConfig.getNodePort(0));

    System.exit(0);
  }

  @SuppressWarnings("unchecked")
  @Override
  /**
   * Converts a string representation of a node id into the appropriate node id type.
   */
  public NodeIDType valueOf(String nodeAsString) throws IllegalArgumentException {
    switch (getNodeIDType()) {
      case String:
        return (NodeIDType) nodeAsString;
      case Integer:
        return (NodeIDType) (Integer.valueOf(nodeAsString.trim()));
      case InetAddress:
        try {
          return (NodeIDType) (InetAddress.getByName(nodeAsString.trim()));
        } catch (UnknownHostException e) {
          throw new IllegalArgumentException("Cannot parse node as an InetAddress");
        }
      default:
        throw new IllegalArgumentException("Bad NodeIDType");
    }
  }

  /**
   * Returns the appropriate NodeIDClass corresponding to the NodeIDType.
   *
   * @return NodeIDClass enum
   */
  private NodeIDClass getNodeIDType() {
    NodeIDType node = null;
    Iterator<NodeIDType> nodeIter = this.hostInfoMapping.keySet().iterator();
    if (nodeIter.hasNext() && (node = nodeIter.next()) != null) {
      return NodeIDClass.valueOf(String.class.getSimpleName());
    }
    throw new IllegalArgumentException("Cannot determine node id class");
  }

  private enum NodeIDClass {

    String, Integer, InetAddress
  }

  ///
  /// READING AND RECHECKING OF HOSTS FILE
  ///
  /**
   *
   * Read a host file to create a mapping of node information for name servers.
   *
   * @param hostsFile
   * @param nameServerID
   * @throws NumberFormatException
   */
  private void readHostsFile(String hostsFile) throws IOException {
    List<HostSpec> hosts = null;
    try {
      hosts = HostFileLoader.loadHostFile(hostsFile);
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException("Problem loading hosts file: " + e);
    }
    // save the old one... maybe we'll need it again?
    previousHostInfoMapping = hostInfoMapping;
    // Create a new one so we don't hose the old one if the new file is bogus
    ConcurrentMap<NodeIDType, HostInfo> newHostInfoMapping = new ConcurrentHashMap<NodeIDType, HostInfo>(16, 0.75f, 8);
    for (HostSpec<NodeIDType> spec : hosts) {
      newHostInfoMapping.put(spec.getId(), new HostInfo(spec.getId(), spec.getName(),
              spec.getStartPort() != null ? spec.getStartPort() : GNS.STARTINGPORT, 0, 0, 0));
    }
    // some idiot checking of the given Id
    if (!isLocalNameServer) {
      HostInfo nodeInfo = newHostInfoMapping.get(this.nodeID);
      if (nodeInfo == null) {
        throw new IOException("NodeId not found in hosts file:" + this.nodeID.toString());
      }
    }
    // ok.. things are cool... actually update (do we need to lock this)
    hostInfoMapping = newHostInfoMapping;
    ConsistentHashing.reInitialize(GNS.numPrimaryReplicas, getNodeIDs());
  }

  private static final long updateCheckPeriod = 60000; // 60 seconds

  private TimerTask timerTask = null;

  private void startCheckingForUpdates() {
    Timer t = new Timer();
    t.scheduleAtFixedRate(
            timerTask = new TimerTask() {
              @Override
              public void run() {
                checkForUpdates();
              }
            },
            updateCheckPeriod, // run first occurrence later
            updateCheckPeriod);
    GNS.getLogger().info("Checking for hosts updates every " + updateCheckPeriod / 1000 + " seconds");
  }

  private void checkForUpdates() {
    try {
      GNS.getLogger().info("Checking for hosts update");
      if (HostFileLoader.isChangedFileVersion(hostsFile)) {
        GNS.getLogger().info("Reading updated hosts file");
        readHostsFile(hostsFile);
      }
    } catch (IOException e) {
      GNS.getLogger().severe("Problem reading hosts file:" + e);
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
        throw new IOException("Hosts file is empty" + file);
      }
      return line.split("\\s+").length > 4;
    } catch (IOException e) {
      GNS.getLogger().severe("Problem reading hosts file:" + e);
      return false;
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
  public void addHostInfo(NodeIDType id, String ipAddress, int startingPort, long pingLatency, double latitude, double longitude) {
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
  public void addHostInfo(NodeIDType id, String ipAddress, Integer startingPort) {
    HostInfo nodeInfo = new HostInfo(id, ipAddress, startingPort != null ? startingPort : GNS.STARTINGPORT, 0, 0, 0);
    GNS.getLogger().fine(nodeInfo.toString());
    hostInfoMapping.put(id, nodeInfo);
  }

  @Override
  public void shutdown() {
    if (timerTask != null) {
      timerTask.cancel();
    }
  }

  @Override
  public String toString() {
    return "GNSNodeConfig{" + "nodeID=" + nodeID + ", isLocalNameServer=" + isLocalNameServer + '}';
  }

}
