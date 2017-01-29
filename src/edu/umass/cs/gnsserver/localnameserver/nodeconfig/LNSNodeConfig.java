

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


public class LNSNodeConfig implements NodeConfig<InetSocketAddress>,
        GNSInterfaceNodeConfig<InetSocketAddress>, Shutdownable {


  public static final long INVALID_PING_LATENCY = -1L;


  public static final int INVALID_PORT = -1;

  private long version = 0l;
  //private final String hostsFile;


  private ConcurrentMap<Object, LNSNodeInfo> hostInfoMapping;


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
    //this.hostsFile = null;
  }


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


  private LNSNodeInfo getNodeInfoForAnyNode(InetSocketAddress address) {
    for (LNSNodeInfo hostInfo : hostInfoMapping.values()) {
      if (hostInfo.getIpAddress().equals(address.getAddress())) {
        return hostInfo;
      }
    }
    return null;
  }


  public int getNumberOfNodes() {
    return hostInfoMapping.size();
  }


  @Override
  public long getPingLatency(InetSocketAddress address) {
    LNSNodeInfo nodeInfo = getNodeInfoForAnyNode(address);
    return (nodeInfo == null) ? INVALID_PING_LATENCY : nodeInfo
            .getPingLatency();
  }


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


  @Override
  public boolean nodeExists(InetSocketAddress address) {
    return address instanceof InetSocketAddress
            && getNodeInfoForAnyNode(address) != null;
  }


  @Override
  public InetAddress getNodeAddress(InetSocketAddress address) {
    if (address instanceof InetSocketAddress) {
      return address.getAddress();
    } else {
      return null;
    }
  }


  @Override
  public InetAddress getBindAddress(InetSocketAddress address) {
    if (address instanceof InetSocketAddress) {
      return address.getAddress();
    } else {
      return null;
    }
  }


  @Override
  public int getNodePort(InetSocketAddress address) {
    if (address instanceof InetSocketAddress) {
      return address.getPort();
    } else {
      return -1;
    }
  }


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
//    ConcurrentMap<Object, LNSNodeInfo> previousHostInfoMapping = hostInfoMapping;
//    // Create a new one so we don't hose the old one if the new file is
//    // bogus
//    ConcurrentMap<Object, LNSNodeInfo> newHostInfoMapping = new ConcurrentHashMap<>(
//            16, 0.75f, 8);
//    for (HostSpec spec : hosts) {
//      addHostInfo(newHostInfoMapping, spec.getId(), spec.getName(),
//              spec.getExternalIP(),
//              spec.getStartPort() != null ? spec.getStartPort()
//                      : OldHackyConstants.DEFAULT_STARTING_PORT);
//    }
//    // ok.. things are cool... actually update
//    hostInfoMapping = newHostInfoMapping;
//    // ConsistentHashing.reInitialize(GNS.numPrimaryReplicas, getNodeIDs());
//  }


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

  //private static final long UPDATE_CHECK_PERIOD = 60000; // 60 seconds

  //private TimerTask timerTask = null;

//  private void startCheckingForUpdates() {
//    Timer t = new Timer();
//    t.scheduleAtFixedRate(timerTask = new TimerTask() {
//      @Override
//      public void run() {
//        checkForUpdates();
//      }
//    }, UPDATE_CHECK_PERIOD, // run first occurrence later
//            UPDATE_CHECK_PERIOD);
//    GNSConfig.getLogger().log(Level.INFO,
//            "Checking for hosts updates every {0} seconds", UPDATE_CHECK_PERIOD / 1000);
//  }

//  private void checkForUpdates() {
//    try {
//      GNSConfig.getLogger().fine("Checking for hosts update");
//      if (HostFileLoader.isChangedFileVersion(hostsFile)) {
//        GNSConfig.getLogger().info("Reading updated hosts file");
//        readHostsFile(hostsFile);
//      }
//    } catch (IOException e) {
//      GNSConfig.getLogger().log(Level.SEVERE,
//              "Problem reading hosts file:{0}", e);
//    }
//
//  }


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


  @Override
  public void shutdown() {
//    if (timerTask != null) {
//      timerTask.cancel();
//    }
  }

  @Override
  public String toString() {
    return "LNSNodeConfig{" + "version=" + version + ", hostInfoMapping="
            + hostInfoMapping + '}';
  }


  @Override
  public InetSocketAddress valueOf(String strValue) {
    throw new UnsupportedOperationException("Not supported yet.");
  }


  @Override
  public Set<InetSocketAddress> getValuesFromStringSet(Set<String> strNodes) {
    throw new UnsupportedOperationException("Not supported yet.");
  }


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
