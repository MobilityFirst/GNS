/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.InterfaceNodeConfig;
import edu.umass.cs.gns.nodeconfig.HostFileLoader;
import edu.umass.cs.gns.nodeconfig.HostSpec;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.Shutdownable;
import edu.umass.cs.gns.util.Stringifiable;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.json.JSONArray;
import org.json.JSONException;

/**
 * This class maintains information that allows the LNS to communicate with Active Replicas and Reconfigurators.
 * 
 * It parses a hosts file to gather information about each name server in the system.
 *
 * Also has support for checking to see if the hosts file changes. When that happens the host info is
 * reloaded and the <code>ConsistentHashing.reInitialize</code> method is called.
 *
 * Also contains an implementation of the <code>Stringifiable</code> interface which allows
 * strings to be converted back to NodeIDTypes.
 *
 * The current implementation maintains three NodeIds for each node that is read from the hosts file.
 * A "top-level" id which is identical to the id read from the file, plus to additional ids.
 * One for the activeReplica and one for the reconfigurator. See <code>LNSNodeInfo</code>,
 * <code>addHostInfo</code> and <code>readHostsFile</code> for the details on
 * how those are generated.
 */
public class LNSNodeConfig implements Stringifiable<Object>, Shutdownable, InterfaceNodeConfig<Object> {

  public static final long INVALID_PING_LATENCY = -1L;
  public static final int INVALID_PORT = -1;

  private long version = 0l;
  private final String hostsFile;

  /**
   * Contains information about each name server. <Key = HostID, Value = LNSNodeInfo>
   *
   */
  private ConcurrentMap<Object, LNSNodeInfo> hostInfoMapping;

  /**
   * Creates a GNSNodeConfig for the given nameServerID and initializes it from a name server host file.
   * This supports the new hosts.txt style format.
   *
   * @param hostsFile
   */
  public LNSNodeConfig(String hostsFile) throws IOException {
    this.hostsFile = hostsFile;
    if (isOldStyleFile(hostsFile)) {
      throw new UnsupportedOperationException("THE USE OF OLD STYLE NODE INFO FILES IS NOT LONGER SUPPORTED. FIX THIS FILE: " + hostsFile);
      //initFromOldStyleFile(hostsFile, nameServerID);
    }
    readHostsFile(hostsFile);
    // Informational purposes
    for (Entry<Object, LNSNodeInfo> hostInfoEntry : hostInfoMapping.entrySet()) {
      GNS.getLogger().info("For LNS "
              + " Id: " + hostInfoEntry.getValue().getId().toString()
              + " Host:" + hostInfoEntry.getValue().getIpAddress()
              + " Start Port:" + hostInfoEntry.getValue().getStartingPortNumber());
    }
    startCheckingForUpdates();
  }

  /**
   * Returns the set of active replica addresses.
   *
   * @return
   */
  public Set<InetSocketAddress> getActiveReplicas() {
      Set<InetSocketAddress> result = new HashSet<>();
      for (LNSNodeInfo hostInfo : hostInfoMapping.values()) {
        result.add(new InetSocketAddress(hostInfo.getIpAddress(), 
                hostInfo.getStartingPortNumber() + GNS.PortType.ACTIVE_REPLICA_PORT.getOffset()));
      }
      return result;
  }
  
  /**
   * Returns the set of reconfigurator addresses.
   *
   * @return
   */
  public Set<InetSocketAddress> getReconfigurators() {
    Set<InetSocketAddress> result = new HashSet<>();
      for (LNSNodeInfo hostInfo : hostInfoMapping.values()) {
        result.add(new InetSocketAddress(hostInfo.getIpAddress(), 
                hostInfo.getStartingPortNumber() + GNS.PortType.RECONFIGURATOR_PORT.getOffset()));
      }
      return result;
  }

  /**
   * Returns the "top-level" host ID for any given nodeID.
   *
   * @param id
   * @return
   */
  private LNSNodeInfo getNodeInfoForAnyNode(InetSocketAddress address) {
    for (LNSNodeInfo hostInfo : hostInfoMapping.values()) {
      if (hostInfo.getIpAddress().equals(address.getAddress())) {
        return hostInfo;
      }
    }
    return null;
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
   * Returns the ping latency between two servers.
   * Will return INVALID_PING_LATENCY if the node doesn't exist.
   *
   * @param address
   * @return
   */
  public long getPingLatency(InetSocketAddress address) {
    LNSNodeInfo nodeInfo = getNodeInfoForAnyNode(address);
    return (nodeInfo == null) ? INVALID_PING_LATENCY : nodeInfo.getPingLatency();
  }

  /**
   * Updates the ping latency table for a node.
   * Only valid for top-level nodes.
   *
   * @param address
   * @param responseTime
   */
  public void updatePingLatency(InetSocketAddress address, long responseTime) {
    LNSNodeInfo nodeInfo = getNodeInfoForAnyNode(address);
    if (nodeInfo != null) {
      nodeInfo.setPingLatency(responseTime);
    } else {
      GNS.getLogger().warning("Can't update latency for " + address.toString() + ".");
    }
  }

  /**
   * Returns true if the node exists.
   * Works for "top-level" node ids and active-replica and reconfigurator nodes ids.
   *
   * @param address
   * @return
   */
  @Override
  public boolean nodeExists(Object address) {
    return address instanceof InetSocketAddress && getNodeInfoForAnyNode((InetSocketAddress)address) != null;
  }
  
  @Override
  public InetAddress getNodeAddress(Object address) {
    if (address instanceof InetSocketAddress) {
      return ((InetSocketAddress)address).getAddress();
    } else {
      return null;
    }
  }

  @Override
  public int getNodePort(Object address) {
    if (address instanceof InetSocketAddress) {
      return ((InetSocketAddress)address).getPort();
    } else {
      return -1;
    }
  }

  @Override
  public Set getNodeIDs() {
    throw new UnsupportedOperationException("Not supported.");
  }

  /**
   * Selects the closest Name Server from a set of Name Servers.
   *
   * @param servers
   * @return id of closest server or INVALID_NAME_SERVER_ID if one can't be found
   */
  public InetSocketAddress getClosestServer(Set<InetSocketAddress> servers) {
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
  public InetSocketAddress getClosestServer(Set<InetSocketAddress> serverIds, Set<InetSocketAddress> excludeServers) {
    if (serverIds == null || serverIds.isEmpty()) {
      return null;
    }

    long lowestLatency = Long.MAX_VALUE;
    InetSocketAddress serverAddress = null;
    for (InetSocketAddress serverId : serverIds) {
      if (excludeServers != null && excludeServers.contains(serverId)) {
        continue;
      }
      long pingLatency = getPingLatency(serverId);
      if (pingLatency != INVALID_PING_LATENCY && pingLatency < lowestLatency) {
        lowestLatency = pingLatency;
        serverAddress = serverId;
      }
    }
    if (Config.debuggingEnabled) {
      GNS.getLogger().info("Closest server is " + serverAddress);
    }
    return serverAddress;
  }

  // Implement the Stringifiable interface
  @SuppressWarnings("unchecked")
  @Override
  /**
   * Converts a string representation of a node id into the appropriate node id type.
   */
  public Object valueOf(String nodeAsString) throws IllegalArgumentException {
    switch (getObject()) {
      case String:
        return nodeAsString;
      case Integer:
        return Integer.valueOf(nodeAsString.trim());
      case InetAddress:
        try {
          return InetAddress.getByName(nodeAsString.trim());
        } catch (UnknownHostException e) {
          throw new IllegalArgumentException("Cannot parse node as an InetAddress");
        }
      default:
        throw new IllegalArgumentException("Bad Object");
    }
  }

  @Override
  public Set<Object> getValuesFromStringSet(Set<String> strNodes) {
    Set<Object> nodes = new HashSet<>();
    for (String strNode : strNodes) {
      nodes.add(valueOf(strNode));
    }
    return nodes;
  }

  @Override
  public Set<Object> getValuesFromJSONArray(JSONArray array) throws JSONException {
    Set<Object> nodes = new HashSet<>();
    for (int i = 0; i < array.length(); i++) {
      nodes.add(valueOf(array.getString(i)));
    }
    return nodes;
  }

  /**
   * Returns the appropriate NodeIDClass corresponding to the Object.
   *
   * @return NodeIDClass enum
   */
  private NodeIDClass getObject() {
    //FIXME: FOR NOW WE ONLY SUPPORT STRINGS
    return NodeIDClass.valueOf(String.class.getSimpleName());
  }

  private enum NodeIDClass {

    String, Integer, InetAddress
  }

  public long getVersion() {
    return version;
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
  @SuppressWarnings("unchecked")
  private void readHostsFile(String hostsFile) throws IOException {
    List<HostSpec> hosts = null;
    try {
      hosts = HostFileLoader.loadHostFile(hostsFile);
      version = HostFileLoader.getFileVersion();
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException("Problem loading hosts file: " + e);
    }
    // save the old one... maybe we'll need it again?
    ConcurrentMap<Object, LNSNodeInfo> previousHostInfoMapping = hostInfoMapping;
    // Create a new one so we don't hose the old one if the new file is bogus
    ConcurrentMap<Object, LNSNodeInfo> newHostInfoMapping 
            = new ConcurrentHashMap<Object, LNSNodeInfo>(16, 0.75f, 8);
    for (HostSpec<Object> spec : hosts) {
      addHostInfo(newHostInfoMapping, spec.getId(), spec.getName(), spec.getExternalIP(),
              spec.getStartPort() != null ? spec.getStartPort() : GNS.STARTINGPORT);
    }
    // ok.. things are cool... actually update
    hostInfoMapping = newHostInfoMapping;
    //ConsistentHashing.reInitialize(GNS.numPrimaryReplicas, getNodeIDs());
  }

  /**
   * Adds a LNSNodeInfo object to the list maintained by this config instance.
   *
   * @param id
   * @param ipAddress
   * @param startingPort
   * @param pingLatency
   * @param latitude
   * @param longitude
   */
  private void addHostInfo(ConcurrentMap<Object, LNSNodeInfo> mapping, Object id, String ipAddress,
          String externalIP, int startingPort, long pingLatency, double latitude, double longitude) {
    // FIXME: THIS IS GOING TO BLOW UP FOR NON-STRING IDS!
    String idString = id.toString();
    Object activeReplicaID = valueOf(idString + "_ActiveReplica");
    Object ReconfiguratorID = valueOf(idString + "_Reconfigurator");
    LNSNodeInfo nodeInfo = new LNSNodeInfo(id, activeReplicaID, ReconfiguratorID,
            ipAddress, externalIP, startingPort, pingLatency, latitude, longitude);
    GNS.getLogger().fine(nodeInfo.toString());
    mapping.put(id, nodeInfo);
  }

  /**
   * Adds a LNSNodeInfo object to the list maintained by this config instance.
   *
   * @param id
   * @param ipAddress
   */
  private void addHostInfo(ConcurrentMap<Object, LNSNodeInfo> mapping, Object id, String ipAddress,
          String externalIP, Integer startingPort) {
    addHostInfo(mapping, id, ipAddress, externalIP, startingPort != null ? startingPort : GNS.STARTINGPORT, 0, 0, 0);
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

  @Override
  public void shutdown() {
    if (timerTask != null) {
      timerTask.cancel();
    }
  }

  @Override
  public String toString() {
    return "LNSNodeConfig{" + "version=" + version + ", hostInfoMapping=" + hostInfoMapping + '}';
  }

  /**
   * Tests
   *
   * @param args
   * @throws java.lang.Exception
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    String filename = Config.WESTY_GNS_DIR_PATH + "/conf/name-server-info";
    LNSNodeConfig gnsNodeConfig = new LNSNodeConfig(filename);
    System.out.println(gnsNodeConfig.hostInfoMapping.toString());
    System.out.println(gnsNodeConfig.getNumberOfNodes());
    System.out.println(gnsNodeConfig.getActiveReplicas());
    System.out.println(gnsNodeConfig.getReconfigurators());
    System.exit(0);
  }
}
