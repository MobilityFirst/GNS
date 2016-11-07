/* Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): Westy */

package edu.umass.cs.gnsserver.localnameserver.nodeconfig;

import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.main.OldHackyConstants;
import edu.umass.cs.gnsserver.nodeconfig.GNSInterfaceNodeConfig;
import edu.umass.cs.gnsserver.nodeconfig.HostFileLoader;
import edu.umass.cs.gnsserver.nodeconfig.HostSpec;
import edu.umass.cs.gnsserver.utils.Shutdownable;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import org.json.JSONArray;
import org.json.JSONException;

/**
 * This class maintains information that allows the LNS to communicate with
 * Active Replicas and Reconfigurators.
 *
 * It parses a hosts file to gather information about each name server in the
 * system.
 *
 * Also has support for checking to see if the hosts file changes. When that
 * happens the host info is reloaded and the
 * <code>ConsistentHashing.reInitialize</code> method is called.
 *
 * Also contains an implementation of the <code>Stringifiable</code> interface
 * which allows strings to be converted back to NodeIDTypes.
 *
 * The current implementation maintains three NodeIds for each node that is read
 * from the hosts file. A "top-level" id which is identical to the id read from
 * the file, plus to additional ids. One for the activeReplica and one for the
 * reconfigurator. See <code>LNSNodeInfo</code>, <code>addHostInfo</code> and
 * <code>readHostsFile</code> for the details on how those are generated.
 */
public class LNSNodeConfig implements NodeConfig<InetSocketAddress>,
        GNSInterfaceNodeConfig<InetSocketAddress>, Shutdownable {

  /**
   * Represents an invalid ping latency.
   */
  public static final long INVALID_PING_LATENCY = -1L;

  /**
   * Represents an invalid port number.
   */
  public static final int INVALID_PORT = -1;

  private long version = 0l;
  private final String hostsFile;

  /**
   * Contains information about each name server. <Key = HostID, Value =
   * LNSNodeInfo>
   *
   */
  private ConcurrentMap<Object, LNSNodeInfo> hostInfoMapping;

  /**
   * arun: LNS doesn't need either of active replica IDs or all of their IPs.
   * LNSAsyncClient obviates all such information. LNS only needs to know of
   * reconfigurator addresses, or minimally, at least one alive reconfigurator
   * address.
   */
  public LNSNodeConfig() {
    Set<InetSocketAddress> reconfigurators = new HashSet<>(
            ReconfigurationConfig.getReconfiguratorAddresses());
    hostInfoMapping = new ConcurrentHashMap<>();
    for (InetSocketAddress address : reconfigurators) {
      addHostInfo(hostInfoMapping, address.toString(), address
              .getAddress().toString(), address.getAddress().toString(),
              address.getPort() > 0 ? address.getPort()
                      : OldHackyConstants.DEFAULT_STARTING_PORT);
    }
    GNSConfig.getLogger().log(Level.INFO, "LNS mapping = {0}", this.hostInfoMapping);
    this.hostsFile = null;
  }

  /**
   * Creates a LNSNodeConfig initializes it from a name server host file. This
   * supports the new hosts.txt style format.
   *
   * @param hostsFile
   * @throws java.io.IOException
   */
//  public LNSNodeConfig(String hostsFile) throws IOException {
//    this.hostsFile = hostsFile;
//    if (isOldStyleFile(hostsFile)) {
//      throw new UnsupportedOperationException(
//              "THE USE OF OLD STYLE NODE INFO FILES IS NOT LONGER SUPPORTED. FIX THIS FILE: "
//              + hostsFile);
//      // initFromOldStyleFile(hostsFile, nameServerID);
//    }
//    readHostsFile(hostsFile);
//    // Informational purposes
//    for (Entry<Object, LNSNodeInfo> hostInfoEntry : hostInfoMapping
//            .entrySet()) {
//      GNSConfig.getLogger().log(Level.INFO, "For LNS "
//              + " Id: {0} Host:{1} Start Port:{2}",
//              new Object[]{hostInfoEntry.getValue().getId().toString(),
//                hostInfoEntry.getValue().getIpAddress(),
//                hostInfoEntry.getValue().getStartingPortNumber()});
//    }
//    startCheckingForUpdates();
//  }

  /**
   * Returns the set of active replica addresses.
   *
   * @return a set of addresses
   */
  @Override
  public Set<InetSocketAddress> getActiveReplicas() {
    Set<InetSocketAddress> result = new HashSet<>();
    for (LNSNodeInfo hostInfo : hostInfoMapping.values()) {
      result.add(new InetSocketAddress(hostInfo.getIpAddress(), hostInfo
              .getStartingPortNumber()
              + OldHackyConstants.PortType.ACTIVE_REPLICA_PORT.getOffset()));
    }
    return result;
  }

  /**
   * Returns the set of reconfigurator addresses.
   *
   * @return a set of addresses
   */
  @Override
  public Set<InetSocketAddress> getReconfigurators() {
    Set<InetSocketAddress> result = new HashSet<>();
    for (LNSNodeInfo hostInfo : hostInfoMapping.values()) {
      result.add(new InetSocketAddress(hostInfo.getIpAddress(), hostInfo
              .getStartingPortNumber()
              + OldHackyConstants.PortType.RECONFIGURATOR_PORT.getOffset()));
    }
    return result;
  }

  /**
   * Returns the "top-level" host ID for any given nodeID.
   *
   * @param id
   * @return the node info
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
   * Returns the ping latency between two servers. Will return
   * INVALID_PING_LATENCY if the node doesn't exist.
   *
   * @param address
   * @return a long
   */
  @Override
  public long getPingLatency(InetSocketAddress address) {
    LNSNodeInfo nodeInfo = getNodeInfoForAnyNode(address);
    return (nodeInfo == null) ? INVALID_PING_LATENCY : nodeInfo
            .getPingLatency();
  }

  /**
   * Updates the ping latency table for a node. Only valid for top-level
   * nodes.
   *
   * @param address
   * @param responseTime
   */
  @Override
  public void updatePingLatency(InetSocketAddress address, long responseTime) {
    LNSNodeInfo nodeInfo = getNodeInfoForAnyNode(address);
    if (nodeInfo != null) {
      nodeInfo.setPingLatency(responseTime);
    } else {
      GNSConfig.getLogger().log(Level.WARNING,
              "Can''t update latency for {0}.", address.toString());
    }
  }

  /**
   * Returns true if the node exists. Works for "top-level" node ids and
   * active-replica and reconfigurator nodes ids.
   *
   * @param address
   * @return true if the node exists
   */
  @Override
  public boolean nodeExists(InetSocketAddress address) {
    return address instanceof InetSocketAddress
            && getNodeInfoForAnyNode(address) != null;
  }

  /**
   *
   * @param address
   * @return an address
   */
  @Override
  public InetAddress getNodeAddress(InetSocketAddress address) {
    if (address instanceof InetSocketAddress) {
      return address.getAddress();
    } else {
      return null;
    }
  }

  /**
   *
   * @param address
   * @return an address
   */
  @Override
  public InetAddress getBindAddress(InetSocketAddress address) {
    if (address instanceof InetSocketAddress) {
      return address.getAddress();
    } else {
      return null;
    }
  }

  /**
   *
   * @param address
   * @return the port
   */
  @Override
  public int getNodePort(InetSocketAddress address) {
    if (address instanceof InetSocketAddress) {
      return address.getPort();
    } else {
      return -1;
    }
  }

  /**
   *
   * @return a set of addresses
   */
  @Override
  public Set<InetSocketAddress> getNodeIDs() {
    return getActiveReplicas();
  }

  @Override
  public long getVersion() {
    return version;
  }

  // /
  // / READING AND RECHECKING OF HOSTS FILE
  // /
  /**
   *
   * Read a host file to create a mapping of node information for name
   * servers.
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
    // Create a new one so we don't hose the old one if the new file is
    // bogus
    ConcurrentMap<Object, LNSNodeInfo> newHostInfoMapping = new ConcurrentHashMap<>(
            16, 0.75f, 8);
    for (HostSpec spec : hosts) {
      addHostInfo(newHostInfoMapping, spec.getId(), spec.getName(),
              spec.getExternalIP(),
              spec.getStartPort() != null ? spec.getStartPort()
                      : OldHackyConstants.DEFAULT_STARTING_PORT);
    }
    // ok.. things are cool... actually update
    hostInfoMapping = newHostInfoMapping;
    // ConsistentHashing.reInitialize(GNS.numPrimaryReplicas, getNodeIDs());
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
  private void addHostInfo(ConcurrentMap<Object, LNSNodeInfo> mapping,
          Object id, String ipAddress, String externalIP, int startingPort,
          long pingLatency, double latitude, double longitude) {
    // FIXME: THIS IS GOING TO BLOW UP FOR NON-STRING IDS!
    String idString = id.toString();
    Object activeReplicaID = idString + "_Repl";
    Object ReconfiguratorID = idString + "_Recon";
    LNSNodeInfo nodeInfo = new LNSNodeInfo(id, activeReplicaID,
            ReconfiguratorID, ipAddress, externalIP, startingPort,
            pingLatency, latitude, longitude);
    GNSConfig.getLogger().fine(nodeInfo.toString());
    mapping.put(id, nodeInfo);
  }

  /**
   * Adds a LNSNodeInfo object to the list maintained by this config instance.
   *
   * @param id
   * @param ipAddress
   */
  private void addHostInfo(ConcurrentMap<Object, LNSNodeInfo> mapping,
          Object id, String ipAddress, String externalIP, Integer startingPort) {
    addHostInfo(
            mapping,
            id,
            ipAddress,
            externalIP,
            startingPort != null ? startingPort : OldHackyConstants.DEFAULT_STARTING_PORT,
            0, 0, 0);
  }

  private static final long UPDATE_CHECK_PERIOD = 60000; // 60 seconds

  private TimerTask timerTask = null;

  private void startCheckingForUpdates() {
    Timer t = new Timer();
    t.scheduleAtFixedRate(timerTask = new TimerTask() {
      @Override
      public void run() {
        checkForUpdates();
      }
    }, UPDATE_CHECK_PERIOD, // run first occurrence later
            UPDATE_CHECK_PERIOD);
    GNSConfig.getLogger().log(Level.INFO,
            "Checking for hosts updates every {0} seconds", UPDATE_CHECK_PERIOD / 1000);
  }

  private void checkForUpdates() {
    try {
      GNSConfig.getLogger().fine("Checking for hosts update");
      if (HostFileLoader.isChangedFileVersion(hostsFile)) {
        GNSConfig.getLogger().info("Reading updated hosts file");
        readHostsFile(hostsFile);
      }
    } catch (IOException e) {
      GNSConfig.getLogger().log(Level.SEVERE,
              "Problem reading hosts file:{0}", e);
    }

  }

  /**
   * Returns true if the file is the old style (has lots of fields).
   *
   * @param file
   * @return true if the file is the old style
   */
  private boolean isOldStyleFile(String file) throws IOException {
    try {
      BufferedReader reader = new BufferedReader(new FileReader(file));
      if (!reader.ready()) {
        throw new IOException("Problem reading host config file "
                + file);
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

  /**
   * Handles LNS shutdown.
   */
  @Override
  public void shutdown() {
    if (timerTask != null) {
      timerTask.cancel();
    }
  }

  @Override
  public String toString() {
    return "LNSNodeConfig{" + "version=" + version + ", hostInfoMapping="
            + hostInfoMapping + '}';
  }

  /**
   *
   * @param strValue
   * @return an address
   */
  @Override
  public InetSocketAddress valueOf(String strValue) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  /**
   *
   * @param strNodes
   * @return a set of addresses
   */
  @Override
  public Set<InetSocketAddress> getValuesFromStringSet(Set<String> strNodes) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  /**
   *
   * @param array
   * @return a set of addresses
   * @throws JSONException
   */
  @Override
  public Set<InetSocketAddress> getValuesFromJSONArray(JSONArray array)
          throws JSONException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int getAdminPort(InetSocketAddress id) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int getCcpPort(InetSocketAddress id) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int getCcpAdminPort(InetSocketAddress id) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
