
package edu.umass.cs.gnsserver.nodeconfig;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.main.OldHackyConstants;
import edu.umass.cs.gnsserver.utils.GNSShutdownable;
import edu.umass.cs.nio.nioutils.InterfaceDelayEmulator;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;


public class GNSNodeConfig<NodeIDType> implements GNSInterfaceNodeConfig<NodeIDType>, GNSShutdownable, InterfaceDelayEmulator<NodeIDType> {


  public static final long INVALID_PING_LATENCY = -1L;


  public static final int INVALID_PORT = -1;

  private long version = 0l;
  private NodeIDType nodeID; // if this is null you should check isCPP; otherwise it might be invalid
  private boolean isCPP = false;
  //private final String hostsFile;


  private ConcurrentMap<NodeIDType, NodeInfo<NodeIDType>> hostInfoMapping;

  // arun: to correct legacy hack
  private boolean addSuffix = false;


  public GNSNodeConfig() throws IOException {
    Map<String, InetSocketAddress> activeMap = PaxosConfig.getActives();
    hostInfoMapping = new ConcurrentHashMap<NodeIDType, NodeInfo<NodeIDType>>();
    for (String active : activeMap.keySet()) {
      addHostInfo(hostInfoMapping, valueOf(active),
              activeMap.get(active).getAddress().toString(), activeMap
              .get(active).getAddress().toString(), activeMap
              .get(active).getPort() > 0 ? activeMap.get(active)
              .getPort() : OldHackyConstants.DEFAULT_STARTING_PORT);
    }
    //hostsFile = null;
  }


  public GNSNodeConfig(String hostsFile, NodeIDType nameServerID) throws IOException {
    if (nameServerID == null) {
      this.isCPP = true;
    }
    this.nodeID = nameServerID;

    //this.hostsFile = hostsFile;
    if (isOldStyleFile(hostsFile)) {
      throw new UnsupportedOperationException("THE USE OF OLD STYLE NODE INFO FILES IS NOT LONGER SUPPORTED. FIX THIS FILE: " + hostsFile);
      //initFromOldStyleFile(hostsFile, nameServerID);
    }
    //readHostsFile(hostsFile);
    // Informational purposes
    for (Entry<NodeIDType, NodeInfo<NodeIDType>> hostInfoEntry : hostInfoMapping.entrySet()) {
      GNSConfig.getLogger().log(Level.INFO,
              "For {0} Id: {1} Host Name:{2} IP:{3} Start Port:{4}",
              new Object[]{nameServerID == null ? "CPP" : nameServerID.toString(),
                hostInfoEntry.getValue().getId().toString(),
                hostInfoEntry.getValue().getIpAddress(),
                hostInfoEntry.getValue().getExternalIPAddress(),
                hostInfoEntry.getValue().getStartingPortNumber()});
    }
    //startCheckingForUpdates();
  }

//
//  public GNSNodeConfig(String hostsFile, boolean isCCP) throws IOException {
//    this(hostsFile, null);
//  }

  // Currently only used by GNSINstaller

//  public GNSNodeConfig() {
//    this.nodeID = null;
//    hostsFile = null;
//  }

  @Override
  public Set<NodeIDType> getNodeIDs() {
    return hostInfoMapping.keySet();
  }


  @Override
  public Set<NodeIDType> getActiveReplicas() {
    Set<NodeIDType> result = new HashSet<>();
    for (NodeInfo<NodeIDType> hostInfo : hostInfoMapping.values()) {
      result.add(hostInfo.getActiveReplicaID());
    }
    return result;
  }

  private NodeInfo<NodeIDType> getActiveReplicaInfo(NodeIDType id) {
    for (NodeInfo<NodeIDType> hostInfo : hostInfoMapping.values()) {
      if (hostInfo.getActiveReplicaID().equals(id)) {
        return hostInfo;
      }
    }
    return null;
  }


  public boolean isActiveReplica(NodeIDType id) {
    return getActiveReplicaInfo(id) != null;
  }


  @Override
  public Set<NodeIDType> getReconfigurators() {
    Set<NodeIDType> result = new HashSet<>();
    for (NodeInfo<NodeIDType> hostInfo : hostInfoMapping.values()) {
      result.add(hostInfo.getReconfiguratorID());
    }
    return result;
  }

  private NodeInfo<NodeIDType> getReconfiguratorInfo(NodeIDType id) {
    for (NodeInfo<NodeIDType> hostInfo : hostInfoMapping.values()) {
      if (hostInfo.getReconfiguratorID().equals(id)) {
        return hostInfo;
      }
    }
    return null;
  }


  private NodeInfo<NodeIDType> getNodeInfoForAnyNode(NodeIDType id) {
    for (NodeInfo<NodeIDType> hostInfo : hostInfoMapping.values()) {
      if (hostInfo.getId().equals(id)
              || hostInfo.getActiveReplicaID().equals(id)
              || hostInfo.getReconfiguratorID().equals(id)) {
        return hostInfo;
      }
    }
    return null;
  }

  private NodeInfo<NodeIDType> getNodeInfoForTopLevelNode(NodeIDType id) {
    for (NodeInfo<NodeIDType> hostInfo : hostInfoMapping.values()) {
      if (hostInfo.getId().equals(id)) {
        return hostInfo;
      }
    }
    return null;
  }


  public NodeIDType getReplicaNodeIdForTopLevelNode(NodeIDType id) {
    NodeInfo<NodeIDType> nodeInfo;
    if ((nodeInfo = getNodeInfoForTopLevelNode(id)) != null) {
      return nodeInfo.getActiveReplicaID();
    }
    return null;
  }


  public NodeIDType getReconfiguratorNodeIdForTopLevelNode(NodeIDType id) {
    NodeInfo<NodeIDType> nodeInfo;
    if ((nodeInfo = getNodeInfoForTopLevelNode(id)) != null) {
      return nodeInfo.getReconfiguratorID();
    }
    return null;
  }


  public int getNumberOfNodes() {
    return hostInfoMapping.size();
  }


  @Override
  public int getNodePort(NodeIDType id) {
    // handle special case for CCP node
    if (id instanceof InetSocketAddress) {
      return ((InetSocketAddress) id).getPort();
    }
    NodeInfo<NodeIDType> nodeInfo = hostInfoMapping.get(id), copy = nodeInfo;
    if ((nodeInfo = getActiveReplicaInfo(id)) != null) {
      return nodeInfo.getStartingPortNumber() + OldHackyConstants.PortType.ACTIVE_REPLICA_PORT.getOffset();
      // Special case for Reconfigurator
    } else if ((nodeInfo = getReconfiguratorInfo(id)) != null) {
      return nodeInfo.getStartingPortNumber() + OldHackyConstants.PortType.RECONFIGURATOR_PORT.getOffset();
    } // arun: the above takes precedence
    else if ((nodeInfo = copy) != null) {
      return nodeInfo.getStartingPortNumber() + OldHackyConstants.PortType.NS_TCP_PORT.getOffset();
      // Special case for ActiveReplica
    } else {
      GNSConfig.getLogger().log(Level.WARNING, "NodeId {0} not a valid Id!", id.toString());
      return INVALID_PORT;
    }
  }


  @Override
  public int getAdminPort(NodeIDType id) {
    NodeInfo<NodeIDType> nodeInfo = getNodeInfoForAnyNode(id);
    return (nodeInfo == null) ? INVALID_PORT : nodeInfo.getStartingPortNumber()
            + OldHackyConstants.PortType.NS_ADMIN_PORT.getOffset();
  }

  @Override
  public int getCcpPort(NodeIDType id) {
    NodeInfo<NodeIDType> nodeInfo = getNodeInfoForAnyNode(id);
    if (nodeInfo != null) {
      return nodeInfo.getStartingPortNumber() + OldHackyConstants.PortType.CCP_PORT.getOffset();
    } else {
      return INVALID_PORT;
    }
  }

  @Override
  public int getCcpAdminPort(NodeIDType id) {
    NodeInfo<NodeIDType> nodeInfo = getNodeInfoForAnyNode(id);
    if (nodeInfo != null) {
      return nodeInfo.getStartingPortNumber() + OldHackyConstants.PortType.CCP_ADMIN_PORT.getOffset();
    } else {
      return INVALID_PORT;
    }
  }


  @Override
  public InetAddress getNodeAddress(NodeIDType id) {
    // handle special case for CCP node
    if (id instanceof InetSocketAddress) {
      return ((InetSocketAddress) id).getAddress();
    }
    NodeInfo<NodeIDType> nodeInfo = hostInfoMapping.get(id);
    if (nodeInfo != null) {
      return nodeInfo.getExternalIPAddress();
      // Special case for ActiveReplica
    } else if ((nodeInfo = getActiveReplicaInfo(id)) != null) {
      return nodeInfo.getExternalIPAddress();
      // Special case for Reconfigurator
    } else if ((nodeInfo = getReconfiguratorInfo(id)) != null) {
      return nodeInfo.getExternalIPAddress();
    } else {
      return null;
    }
  }


  @Override
  public InetAddress getBindAddress(NodeIDType id) {
    // handle special case for CCP node
    if (id instanceof InetSocketAddress) {
      return ((InetSocketAddress) id).getAddress();
    }
    NodeInfo<NodeIDType> nodeInfo = hostInfoMapping.get(id);
    if (nodeInfo != null) {
      return nodeInfo.getIpAddress();
      // Special case for ActiveReplica
    } else if ((nodeInfo = getActiveReplicaInfo(id)) != null) {
      return nodeInfo.getIpAddress();
      // Special case for Reconfigurator
    } else if ((nodeInfo = getReconfiguratorInfo(id)) != null) {
      return nodeInfo.getIpAddress();
    } else {
      return null;
    }
  }


  @Override
  public long getPingLatency(NodeIDType id) {
    NodeInfo<NodeIDType> nodeInfo = getNodeInfoForAnyNode(id);
    return (nodeInfo == null) ? INVALID_PING_LATENCY : nodeInfo.getPingLatency();
  }


  @Override
  public void updatePingLatency(NodeIDType id, long responseTime) {
    NodeInfo<NodeIDType> nodeInfo = getNodeInfoForAnyNode(id);
    if (nodeInfo != null) {
      nodeInfo.setPingLatency(responseTime);
    } else {
      GNSConfig.getLogger().log(Level.WARNING, "Can''t update latency for {0}.", id.toString());
    }
  }


  @Override
  public boolean nodeExists(NodeIDType id) {
    return getNodeInfoForAnyNode(id) != null;
  }


  public int getPortForTopLevelNode(NodeIDType nameServerId, OldHackyConstants.PortType portType) {
    switch (portType) {
      case NS_TCP_PORT:
        return getNodePort(nameServerId);
      case NS_ADMIN_PORT:
        return getAdminPort(nameServerId);
      case CCP_PORT:
        return getCcpPort(nameServerId);
      case CCP_ADMIN_PORT:
        return getCcpAdminPort(nameServerId);
    }
    return -1;
  }


  @Deprecated
  public NodeIDType getClosestServer() {
    return GNSNodeConfig.this.getClosestServer(getNodeIDs());
  }


  public NodeIDType getClosestServer(Set<NodeIDType> servers) {
    return getClosestServer(servers, null);
  }


  public NodeIDType getClosestServer(Set<NodeIDType> serverIds, Set<NodeIDType> excludeServers) {
    if (serverIds == null || serverIds.isEmpty()) {
      return null;
    }
    // If the local server is one of the server ids and not excluded return it.
    if (nodeID != null && serverIds.contains(nodeID) && excludeServers != null && !excludeServers.contains(nodeID)) {
      return nodeID;
    }

    long lowestLatency = Long.MAX_VALUE;
    NodeIDType nameServerID = null;
    for (NodeIDType serverId : serverIds) {
      if (excludeServers != null && excludeServers.contains(serverId)) {
        continue;
      }
      long pingLatency = getPingLatency(serverId);
      if (pingLatency != INVALID_PING_LATENCY && pingLatency < lowestLatency) {
        lowestLatency = pingLatency;
        nameServerID = serverId;
      }
    }

    // a little more hair in case all the pings are invalid
    if (nameServerID == null) {
      // return the first one that is not in the exclude list
      for (NodeIDType serverId : serverIds) {
        if (excludeServers != null && excludeServers.contains(serverId)) {
          continue;
        }
        nameServerID = serverId;
        break;
      }
    }
    GNSConfig.getLogger().log(Level.FINE,
            "Closest server is {0} exluded: {1}", new Object[]{nameServerID, excludeServers});

    return nameServerID;
  }

  // Implement the Stringifiable interface

  @SuppressWarnings("unchecked")
  @Override
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


  @Override
  public Set<NodeIDType> getValuesFromStringSet(Set<String> strNodes
  ) {
    Set<NodeIDType> nodes = new HashSet<>();
    for (String strNode : strNodes) {
      nodes.add(valueOf(strNode));
    }
    return nodes;
  }


  @Override
  public Set<NodeIDType> getValuesFromJSONArray(JSONArray array) throws JSONException {
    Set<NodeIDType> nodes = new HashSet<>();
    for (int i = 0; i < array.length(); i++) {
      nodes.add(valueOf(array.getString(i)));
    }
    return nodes;
  }


  private NodeIDClass getNodeIDType() {
    //FIXME: FOR NOW WE ONLY SUPPORT STRINGS
    return NodeIDClass.valueOf(String.class.getSimpleName());
  }

  private enum NodeIDClass {

    String, Integer, InetAddress
  }

  @Override
  public long getVersion() {
    return version;
  }

//  ///
//  /// READING AND RECHECKING OF HOSTS FILE
//  ///
//
//  @SuppressWarnings("unchecked")
//  private void readHostsFile(String hostsFile) throws IOException {
//    List<HostSpec> hosts = null;
//    try {
//      hosts = HostFileLoader.loadHostFile(hostsFile);
//      version = HostFileLoader.getFileVersion();
//    } catch (Exception e) {
//      e.printStackTrace();
//      throw new IOException("Problem loading hosts file: " + e);
//    }
//    // save the old one... maybe we'll need it again?
//    ConcurrentMap<NodeIDType, NodeInfo<NodeIDType>> previousHostInfoMapping = hostInfoMapping;
//    // Create a new one so we don't hose the old one if the new file is bogus
//    ConcurrentMap<NodeIDType, NodeInfo<NodeIDType>> newHostInfoMapping
//            = new ConcurrentHashMap<>(16, 0.75f, 8);
//    for (HostSpec spec : hosts) {
//      addSuffix = true;
//      addHostInfo(newHostInfoMapping, (NodeIDType) spec.getId(), spec.getName(), spec.getExternalIP(),
//              spec.getStartPort() != null ? spec.getStartPort() : OldHackyConstants.DEFAULT_STARTING_PORT);
//    }
//    // some idiot checking of the given Id
//    if (!isCPP) {
//      NodeInfo<NodeIDType> nodeInfo = newHostInfoMapping.get(this.nodeID);
//      if (nodeInfo == null) {
//        throw new IOException("NodeId not found in hosts file:" + this.nodeID.toString());
//      }
//    }
//    // ok.. things are cool... actually update
//    hostInfoMapping = newHostInfoMapping;
//    //ConsistentHashing.reInitialize(GNS.numPrimaryReplicas, getNodeIDs());
//  }


  private void addHostInfo(ConcurrentMap<NodeIDType, NodeInfo<NodeIDType>> mapping, NodeIDType id, String ipAddress,
          String externalIP, int startingPort, long pingLatency, double latitude, double longitude) {
    // FIXME: THIS IS GOING TO BLOW UP FOR NON-STRING IDS!
    String idString = id.toString();
    NodeIDType activeReplicaID = valueOf(idString + (addSuffix ? "_Repl" : ""));
    NodeIDType ReconfiguratorID = valueOf(idString + (addSuffix ? "_Recon" : ""));
    NodeInfo<NodeIDType> nodeInfo = new NodeInfo<>(id, activeReplicaID, ReconfiguratorID,
            ipAddress, externalIP, startingPort, pingLatency, latitude, longitude);
    GNSConfig.getLogger().fine(nodeInfo.toString());
    mapping.put(id, nodeInfo);
  }


  private void addHostInfo(ConcurrentMap<NodeIDType, NodeInfo<NodeIDType>> mapping, NodeIDType id, String ipAddress,
          String externalIP, Integer startingPort) {
    addHostInfo(mapping, id, ipAddress, externalIP, startingPort != null ? startingPort 
            : OldHackyConstants.DEFAULT_STARTING_PORT, 0, 0, 0);
  }

  //private static final long updateCheckPeriod = 60000; // 60 seconds

//  private TimerTask timerTask = null;
//
//  private void startCheckingForUpdates() {
//    Timer t = new Timer();
//    t.scheduleAtFixedRate(
//            timerTask = new TimerTask() {
//      @Override
//      public void run() {
//        checkForUpdates();
//      }
//    },
//            updateCheckPeriod, // run first occurrence later
//            updateCheckPeriod);
//    GNSConfig.getLogger().log(Level.INFO,
//            "Checking for hosts updates every {0} seconds", updateCheckPeriod / 1000);
//  }

//  private void checkForUpdates() {
//    try {
//      GNSConfig.getLogger().fine("Checking for hosts update");
//      if (HostFileLoader.isChangedFileVersion(hostsFile)) {
//        GNSConfig.getLogger().info("Reading updated hosts file");
//        readHostsFile(hostsFile);
//      }
//    } catch (IOException e) {
//      GNSConfig.getLogger().log(Level.SEVERE, "Problem reading hosts file:{0}", e);
//    }
//
//  }


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
      GNSConfig.getLogger().log(Level.SEVERE, "Problem reading hosts file:{0}", e);
      return false;
    }
  }

  @Override
  public void shutdown() {
//    if (timerTask != null) {
//      timerTask.cancel();
//    }
  }

  @Override
  public String toString() {
    return "GNSNodeConfig{" + "nodeID=" + nodeID + ", isLocalNameServer=" + isCPP + '}';
  }


//  @SuppressWarnings("unchecked")
//  public static void main(String[] args) throws Exception {
//    String filename = "conf/single-server-info.txt";
//    GNSNodeConfig<String> gnsNodeConfig = new GNSNodeConfig<>(filename, "frank");
//    System.out.println(gnsNodeConfig.hostInfoMapping.toString());
//    System.out.println(gnsNodeConfig.getNumberOfNodes());
//    System.out.println(gnsNodeConfig.getNodePort("smith"));
//    System.out.println(gnsNodeConfig.getNodeAddress("frank"));
//    System.out.println(gnsNodeConfig.getNodePort("frank"));
//    System.out.println(gnsNodeConfig.getActiveReplicas());
//    System.out.println(gnsNodeConfig.getReconfigurators());
//    System.out.println(gnsNodeConfig.getNodePort("frank_Repl"));
//    System.out.println(gnsNodeConfig.getNodeAddress("billy_Recon"));
//    System.exit(0);
//  }


  @Override
  public long getEmulatedDelay(NodeIDType node2) {
    return this.getPingLatency(node2);
  }
}
