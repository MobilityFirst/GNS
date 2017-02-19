/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy, arun
 *
 */
package edu.umass.cs.gnsserver.nodeconfig;

import com.google.common.collect.ImmutableSet;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.Shutdownable;
import edu.umass.cs.nio.nioutils.InterfaceDelayEmulator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import org.json.JSONArray;
import org.json.JSONException;

/**
 * This class maintains information that allows different nodes to communicate with each other.
 * It parses a hosts file to gather information about each name server in the system.
 *
 * Also has support for checking to see if the hosts file changes. When that happens the host info is
 * reloaded and the <code>ConsistentHashing.reInitialize</code> method is called.
 *
 * Also contains an implementation of the <code>Stringifiable</code> interface which allows
 * strings to be converted back to NodeIDTypes.
 *
 * This also implements InterfaceReconfigurableNodeConfig which entails a separation of the activeReplica
 * and reconfigurator network ports.
 *
 * The current implementation maintains three NodeIds for each node that is read from the hosts file.
 * A "top-level" id which is identical to the id read from the file, plus to additional ids.
 * One for the activeReplica and one for the reconfigurator. See <code>NodeInfo</code>,
 * <code>addHostInfo</code> and <code>readHostsFile</code> for the details on
 * how those are generated.
 *
 * One caveat of the current implementation is that despite the use of NodeIDType throughout the code,
 * we only currently support String NodeIDTypes in this class.
 *
 * @param <NodeIDType>
 */
public class GNSNodeConfig<NodeIDType> implements GNSInterfaceNodeConfig<NodeIDType>, Shutdownable, InterfaceDelayEmulator<NodeIDType> {

  /**
   * An invalid latency.
   */
  public static final long INVALID_PING_LATENCY = -1L;

  /**
   * An invalid port.
   */
  public static final int INVALID_PORT = -1;

  private long version = 0l;
  private NodeIDType nodeID; // if this is null you should check isCPP; otherwise it might be invalid
  private boolean isCPP = false;
  //private final String hostsFile;

  /**
   * Contains information about each name server. <Key = HostID, Value = NodeInfo>
   *
   */
  private final ConcurrentMap<NodeIDType, NodeInfo<NodeIDType>> hostInfoMapping;

  // arun: to correct legacy hack
  private boolean addSuffix = false;

  /**
   * For use with gigapaxos createApp.
   *
   * @throws IOException
   */
  public GNSNodeConfig() throws IOException {
    Map<String, InetSocketAddress> activeMap = PaxosConfig.getActives();
    hostInfoMapping = new ConcurrentHashMap<>();
    for (String active : activeMap.keySet()) {
      addHostInfo(hostInfoMapping, valueOf(active),
              activeMap.get(active).getAddress().toString(),
              activeMap.get(active).getAddress().toString(),
              activeMap.get(active).getPort());
    }
  }

  /**
   * Returns the complete set of "top-level" IDs for all name servers (not local name servers).
   * *** ONLY FOR USE IN INSTRUMENTATION ***
   * "top-level" as in not active-replica or reconfigurator node ids.
   *
   * If you want those use <code>getActiveReplicas</code> and <code>getReconfigurators</code>.
   *
   * @return the set of IDs.
   */
  @Override
  public Set<NodeIDType> getNodeIDs() {
    return ImmutableSet.copyOf(hostInfoMapping.keySet());
  }

  /**
   * Returns the set of active replica ids.
   *
   * @return a set of node ids
   */
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

  /**
   *
   * Returns true if this node is an active replica, otherwise false.
   *
   * @param id
   * @return true if this node is an active replica
   */
  public boolean isActiveReplica(NodeIDType id) {
    return getActiveReplicaInfo(id) != null;
  }

  /**
   * Returns the set of reconfigurator ids.
   *
   * @return a set of node ids
   */
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

  /**
   * Returns the "top-level" host ID for any given nodeID.
   *
   * @param id
   * @return a {@link NodeInfo}
   */
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

  /**
   * Returns the active replica associated with this node id.
   *
   * @param id
   * @return a node id
   */
  public NodeIDType getReplicaNodeIdForTopLevelNode(NodeIDType id) {
    NodeInfo<NodeIDType> nodeInfo;
    if ((nodeInfo = getNodeInfoForTopLevelNode(id)) != null) {
      return nodeInfo.getActiveReplicaID();
    }
    return null;
  }

  /**
   * Returns the reconfigurator associated with this node id.
   *
   * @param id
   * @return a node id
   */
  public NodeIDType getReconfiguratorNodeIdForTopLevelNode(NodeIDType id) {
    NodeInfo<NodeIDType> nodeInfo;
    if ((nodeInfo = getNodeInfoForTopLevelNode(id)) != null) {
      return nodeInfo.getReconfiguratorID();
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
   * Returns the TCP port of a Node.
   * Works for InetSocketAddresses and active replicas.
   * Will return INVALID_NAME_SERVER_ID if the node doesn't exist.
   *
   * @param id
   * @return an int
   */
  @Override
  public int getNodePort(NodeIDType id) {
    // handle special case for CCP node
    if (id instanceof InetSocketAddress) {
      return ((InetSocketAddress) id).getPort();
    }
    NodeInfo<NodeIDType> nodeInfo = hostInfoMapping.get(id);
    if ((nodeInfo = getActiveReplicaInfo(id)) != null) {
      return nodeInfo.getActivePort();
    } else {
      GNSConfig.getLogger().log(Level.WARNING, "NodeId {0} not a valid Id!", id.toString());
      return INVALID_PORT;
    }
  }

  /**
   * Returns the associated Admin port for any node.
   * Will return INVALID_PORT if the node doesn't exist.
   * Works for "top-level" node ids and active-replica and reconfigurator nodes ids.
   *
   * @param id Nameserver id
   * @return the active nameserver information port of a nameserver. *
   */
  @Override
  public int getServerAdminPort(NodeIDType id) {
    NodeInfo<NodeIDType> nodeInfo = getNodeInfoForAnyNode(id);
    return (nodeInfo == null) ? INVALID_PORT
            : nodeInfo.getActivePort()
            + PortOffsets.SERVER_ADMIN_PORT.getOffset();
  }

  @Override
  public int getCollatingAdminPort(NodeIDType id) {
    NodeInfo<NodeIDType> nodeInfo = getNodeInfoForAnyNode(id);
    if (nodeInfo != null) {
      return nodeInfo.getActivePort()
              + PortOffsets.COLLATING_ADMIN_PORT.getOffset();
    } else {
      return INVALID_PORT;
    }
  }

  /**
   * Returns the IP address of a node.
   * Will return null if the node doesn't exist.
   * Works for "top-level" node ids and active-replica and reconfigurator nodes ids.
   *
   * @param id Server id
   * @return IP address of a server
   */
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

  /**
   *
   * @param id
   * @return an address
   */
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

  /**
   * Returns the ping latency between two servers.
   * Will return INVALID_PING_LATENCY if the node doesn't exist.
   *
   * @param id Server id
   * @return a long
   */
  @Override
  public long getPingLatency(NodeIDType id) {
    NodeInfo<NodeIDType> nodeInfo = getNodeInfoForAnyNode(id);
    return (nodeInfo == null) ? INVALID_PING_LATENCY : nodeInfo.getPingLatency();
  }

  /**
   * Updates the ping latency table for a node.
   * Only valid for top-level nodes.
   *
   * @param id
   * @param responseTime
   */
  @Override
  public void updatePingLatency(NodeIDType id, long responseTime) {
    NodeInfo<NodeIDType> nodeInfo = getNodeInfoForAnyNode(id);
    if (nodeInfo != null) {
      nodeInfo.setPingLatency(responseTime);
    } else {
      GNSConfig.getLogger().log(Level.WARNING, "Can''t update latency for {0}.", id.toString());
    }
  }

  /**
   * Returns true if the node exists.
   * Works for "top-level" node ids and active-replica and reconfigurator nodes ids.
   *
   * @param id
   * @return true if the node exists
   */
  @Override
  public boolean nodeExists(NodeIDType id) {
    return getNodeInfoForAnyNode(id) != null;
  }

  /**
   * **
   * Returns port number for the specified port type.
   * Return -1 if the specified port type does not exist.
   * Only for "top-level" nodes, not active-replica and reconfigurator nodes.
   *
   * @param nameServerId Name server id //
   * @param portType	GNS port type
   *
   * @return the port
   */
  public int getPortForTopLevelNode(NodeIDType nameServerId, PortOffsets portType) {
    switch (portType) {
      case SERVER_ADMIN_PORT:
        return getServerAdminPort(nameServerId);
      case COLLATING_ADMIN_PORT:
        return getCollatingAdminPort(nameServerId);
    }
    return -1;
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
  /**
   * Converts a string representation of a node id into the appropriate node id type.
   *
   * @param nodeAsString
   * @return the node id
   */
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

  /**
   *
   * @param strNodes
   * @return a set of nodes
   */
  @Override
  public Set<NodeIDType> getValuesFromStringSet(Set<String> strNodes
  ) {
    Set<NodeIDType> nodes = new HashSet<>();
    for (String strNode : strNodes) {
      nodes.add(valueOf(strNode));
    }
    return nodes;
  }

  /**
   *
   * @param array
   * @return a set of nodes
   * @throws JSONException
   */
  @Override
  public Set<NodeIDType> getValuesFromJSONArray(JSONArray array) throws JSONException {
    Set<NodeIDType> nodes = new HashSet<>();
    for (int i = 0; i < array.length(); i++) {
      nodes.add(valueOf(array.getString(i)));
    }
    return nodes;
  }

  /**
   * Returns the appropriate NodeIDClass corresponding to the NodeIDType.
   *
   * @return NodeIDClass enum
   */
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

  /* Adds a NodeInfo object to the list maintained by this config instance. */
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

  /* Adds a NodeInfo object to the list maintained by this config instance. */
  private void addHostInfo(ConcurrentMap<NodeIDType, NodeInfo<NodeIDType>> mapping, NodeIDType id, String ipAddress,
          String externalIP, Integer startingPort) {
    addHostInfo(mapping, id, ipAddress, externalIP, startingPort, 0, 0, 0);
  }

  @Override
  public void shutdown() {
  }

  @Override
  public String toString() {
    return "GNSNodeConfig{" + "nodeID=" + nodeID + ", isLocalNameServer=" + isCPP + '}';
  }

  /**
   *
   * @param node2
   * @return the delay
   */
  @Override
  public long getEmulatedDelay(NodeIDType node2) {
    return this.getPingLatency(node2);
  }
}
