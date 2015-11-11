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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.localnameserver;

import edu.umass.cs.gnsserver.localnameserver.nodeconfig.LNSNodeConfig;
import edu.umass.cs.gnsserver.localnameserver.nodeconfig.LNSConsistentReconfigurableNodeConfig;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import static edu.umass.cs.gnscommon.GnsProtocol.HELP;
import static edu.umass.cs.gnsserver.localnameserver.nodeconfig.LNSNodeConfig.INVALID_PING_LATENCY;
import static edu.umass.cs.gnsserver.localnameserver.LocalNameServerOptions.NS_FILE;
import static edu.umass.cs.gnsserver.localnameserver.LocalNameServerOptions.PORT;
import static edu.umass.cs.gnsserver.localnameserver.LocalNameServerOptions.disableSSL;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnsserver.ping.PingManager;
import edu.umass.cs.gnsserver.utils.Shutdownable;
import edu.umass.cs.gnscommon.utils.NetworkUtils;
import edu.umass.cs.gnsserver.utils.ParametersAndOptions;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import static edu.umass.cs.gnsserver.utils.ParametersAndOptions.printOptions;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.SSLDataProcessingWorker;
import static edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES.CLEAR;
import static edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES.SERVER_AUTH;
import edu.umass.cs.reconfiguration.ActiveReplica;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class LocalNameServer implements RequestHandlerInterface, Shutdownable {

  /**
   * The default length of time to keep values in the cache.
   */
  public static final int DEFAULT_VALUE_CACHE_TTL = 10000; // milleseconds

  /**
   * The logger.
   */
  public static final Logger LOG = Logger.getLogger(LocalNameServer.class.getName());

  /**
   * The default LNS port for incoming requests.
   */
  public final static int DEFAULT_LNS_TCP_PORT = 24398;

  private static final ConcurrentMap<Integer, LNSRequestInfo> outstandingRequests = new ConcurrentHashMap<>(10, 0.75f, 3);

  private final Cache<String, CacheEntry> cache;

  //private InterfaceNIOTransport<String, JSONObject> tcpTransport;
  private JSONMessenger<InetSocketAddress> messenger;
  private ProtocolExecutor<InetSocketAddress, ReconfigurationPacket.PacketType, String> protocolExecutor;
  private final LNSNodeConfig nodeConfig;
  private final LNSConsistentReconfigurableNodeConfig crNodeConfig;
  private final InetSocketAddress address;
  private final AbstractJSONPacketDemultiplexer demultiplexer;

  /**
   * Determines if additional debugging information is output.
   */
  public static boolean debuggingEnabled = false;
  /**
   * Ping manager object for pinging other nodes and updating ping latencies.
   */
  private final PingManager<InetSocketAddress> pingManager;

  /**
   * Create a LocalNameServer instance.
   *
   * @param nodeAddress
   * @param nodeConfig
   */
  public LocalNameServer(InetSocketAddress nodeAddress, LNSNodeConfig nodeConfig) {
    SSLDataProcessingWorker.SSL_MODES sslMode;
    if (!LocalNameServerOptions.disableSSL) {
      sslMode = SERVER_AUTH;
      ReconfigurationConfig.setClientPortOffset(100);
      // Set up a SERVER_AUTH address the client can use
      nodeAddress = new InetSocketAddress(nodeAddress.getAddress(), nodeAddress.getPort() + 100);
    } else {
      sslMode = CLEAR;
      ReconfigurationConfig.setClientPortOffset(0);
    }
    this.address = nodeAddress;
    System.out.println("LNS: SSL Mode is " + sslMode.name());
    this.nodeConfig = nodeConfig;
    this.crNodeConfig = new LNSConsistentReconfigurableNodeConfig(nodeConfig);
    this.demultiplexer = new LNSPacketDemultiplexer(this);
    this.cache = CacheBuilder.newBuilder().concurrencyLevel(5).maximumSize(1000).build();
    try {
      JSONNIOTransport<InetSocketAddress> gnsNiot = new JSONNIOTransport<>(address, crNodeConfig, demultiplexer, sslMode);
      messenger = new JSONMessenger<InetSocketAddress>(gnsNiot);
      this.protocolExecutor = new ProtocolExecutor<InetSocketAddress, ReconfigurationPacket.PacketType, String>(messenger);
    } catch (IOException e) {
      LOG.info("Unabled to start LNS listener: " + e);
      System.exit(0);
    }
    LOG.info("Started LNS listener on " + address);
//    if (!LocalNameServerOptions.disableSSL) {
//      messenger.setClientMessenger(initClientMessenger());
//    }
    GNS.getLogger().info("LNS running at " + nodeAddress.getHostString() + " started Ping manager.");

    this.pingManager = new PingManager<InetSocketAddress>(crNodeConfig);
    pingManager.startPinging();
  }

  /**
   * Handles LNS shutdown.
   */
  @Override
  public void shutdown() {
    messenger.stop();
    demultiplexer.stop();
    protocolExecutor.stop();
  }

  /**
   * The main routine.
   *
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    Map<String, String> options
            = ParametersAndOptions.getParametersAsHashMap(LocalNameServer.class.getCanonicalName(),
                    LocalNameServerOptions.getAllOptions(), args);
    if (options.containsKey(HELP)) {
      ParametersAndOptions.printUsage(LocalNameServer.class.getCanonicalName(),
              LocalNameServerOptions.getAllOptions());
      System.exit(0);
    }
    printOptions(options);
    LocalNameServerOptions.initializeFromOptions(options);
    try {
      InetSocketAddress address = new InetSocketAddress(NetworkUtils.getLocalHostLANAddress().getHostAddress(),
              options.containsKey(PORT) ? Integer.parseInt(options.get(PORT)) : DEFAULT_LNS_TCP_PORT);
      LocalNameServer lns = new LocalNameServer(address, new LNSNodeConfig(options.get(NS_FILE)));
      //lns.testCache();
    } catch (IOException e) {
      System.out.println("Usage: java -cp GNS.jar edu.umass.cs.gnsserver.localnameserver <nodeConfigFile>");
    }
  }

  /**
   * Returns the protocolExecutor.
   * 
   * @return the protocolExecutor
   */
  @Override
  public ProtocolExecutor<InetSocketAddress, PacketType, String> getProtocolExecutor() {
    return protocolExecutor;
  }

  /**
   * Returns the node config.
   * 
   * @return the node config
   */
  @Override
  public LNSConsistentReconfigurableNodeConfig getNodeConfig() {
    return crNodeConfig;
  }

  /**
   * Returns the node address.
   * 
   * @return the node address
   */
  @Override
  public InetSocketAddress getNodeAddress() {
    return address;
  }

  /**
   * Returns the demultiplexer.
   * @return the demultiplexer
   */
  @Override
  public AbstractJSONPacketDemultiplexer getDemultiplexer() {
    return demultiplexer;
  }

  /**
   * Are we in debug mode?
   * 
   * @return true if we are in debug mode
   */
  @Override
  public boolean isDebugMode() {
    return debuggingEnabled;
  }

  /**
   * Adds the request info.
   * 
   * @param id
   * @param requestInfo
   */
  @Override
  public void addRequestInfo(int id, LNSRequestInfo requestInfo) {
    outstandingRequests.put(id, requestInfo);
  }

  /**
   * Removes the request info.
   * 
   * @param id
   * @return the request info removed
   */
  @Override
  public LNSRequestInfo removeRequestInfo(int id) {
    return outstandingRequests.remove(id);
  }

  /**
   * Returns the request info.
   * 
   * @param id
   * @return the request info
   */
  @Override
  public LNSRequestInfo getRequestInfo(int id) {
    return outstandingRequests.get(id);
  }

  /**
   * Returns the ping manager.
   * 
   * @return the ping manager
   */
  @Override
  public PingManager<InetSocketAddress> getPingManager() {
    return pingManager;
  }

  /**
   * Returns the active replicas.
   * 
   * @param name
   * @return the active replicas
   */
  @Override
  public Set<InetSocketAddress> getReplicatedActives(String name) {
    // FIXME: this needs work
    if (!disableSSL) {
      Set<InetSocketAddress> result = new HashSet<>();
      for (InetSocketAddress socketAddress : crNodeConfig.getReplicatedActives(name)) {
        // If we're doing SSL we need to get the correct SSL port on the server.
        if (!disableSSL) {
          result.add(new InetSocketAddress(socketAddress.getAddress(),
                  ActiveReplica.getClientFacingPort(socketAddress.getPort())));
        }
      }
      return result;
    } else {
      return crNodeConfig.getReplicatedActives(name);
    }
  }

  /**
   * Selects the closest Name Server from a set of Name Servers.
   *
   * @param servers
   * @return id of closest server or INVALID_NAME_SERVER_ID if one can't be found
   */
  @Override
  public InetSocketAddress getClosestReplica(Set<InetSocketAddress> servers) {
    return getClosestReplica(servers, null);
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
  @Override
  public InetSocketAddress getClosestReplica(Set<InetSocketAddress> serverIds, Set<InetSocketAddress> excludeServers) {
    if (serverIds == null || serverIds.isEmpty()) {
      return null;
    }

    long lowestLatency = Long.MAX_VALUE;
    InetSocketAddress serverAddress = null;
    for (InetSocketAddress serverId : serverIds) {
      if (excludeServers != null && excludeServers.contains(serverId)) {
        continue;
      }
      long pingLatency = nodeConfig.getPingLatency(serverId);
      if (pingLatency != INVALID_PING_LATENCY && pingLatency < lowestLatency) {
        lowestLatency = pingLatency;
        serverAddress = serverId;
      }
    }
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      GNS.getLogger().info("Closest server is " + serverAddress);
    }
    return serverAddress;
  }

  /**
   * Updates the value in the cache.
   * 
   * @param name
   * @param value
   */
  @Override
  public void updateCacheEntry(String name, String value) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    if (cacheEntry != null) {
      cacheEntry.updateCacheEntry(value);
    } else {
      CacheEntry entry = new CacheEntry(name, value);
      cache.put(entry.getName(), entry);
    }
  }

  /**
   * Returns the value if it has not timed out.
   * 
   * @param name
   * @return the value
   */
  @Override
  public String getValueIfValid(String name) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    if (cacheEntry != null && cacheEntry.isValidValue()) {
      return cacheEntry.getValue();
    } else {
      return null;
    }
  }

  /**
   * Updates the set of active replicas.
   * 
   * @param name
   * @param actives
   */
  @Override
  public void updateCacheEntry(String name, Set<InetSocketAddress> actives) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    if (cacheEntry != null) {
      cacheEntry.updateCacheEntry(actives);
    } else {
      CacheEntry entry = new CacheEntry(name, actives);
      cache.put(entry.getName(), entry);
    }
  }

  /**
   * Returns the set of active replicas if they have not timed out.
   * 
   * @param name
   * @return a set of active replicas
   */
  @Override
  public Set<InetSocketAddress> getActivesIfValid(String name) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    if (cacheEntry != null && cacheEntry.isValidActives()) {
      return cacheEntry.getActiveNameServers();
    } else {
      return null;
    }
  }

  /**
   * Clears the cache.
   */
  @Override
  public void invalidateCache() {
    cache.invalidateAll();
  }

  /**
   * Returns true if the name is in the cache.
   * 
   * @param name
   * @return true if the name is in the cache
   */
  @Override
  public boolean containsCacheEntry(String name) {
    return cache.getIfPresent(name) != null;
  }

  /**
   * Invokes the protocolExecutor to handle an event.
   * Returns true if the event was handled.
   * 
   * @param json
   * @return true if the event was handled
   * @throws JSONException
   */
  @SuppressWarnings("unchecked")
  @Override
  public boolean handleEvent(JSONObject json) throws JSONException {
    BasicReconfigurationPacket<String> rcEvent
            = (BasicReconfigurationPacket<String>) ReconfigurationPacket.getReconfigurationPacket(json, nodeConfig);
    return this.protocolExecutor.handleEvent(rcEvent);
  }

  /**
   * Sends a JSON packet to the closets active replica.
   * 
   * @param servers
   * @param packet
   * @throws IOException
   */
  @Override
  public void sendToClosestReplica(Set<InetSocketAddress> servers, JSONObject packet) throws IOException {
    InetSocketAddress address = LocalNameServer.this.getClosestReplica(servers);
    // Remove these so the stamper will put new ones in so the packet will find it's way back here.
    packet.remove(MessageNIOTransport.SNDR_IP_FIELD);
    packet.remove(MessageNIOTransport.SNDR_PORT_FIELD);
    // Don't get a client facing port for these because they are returned as already translated.
    if (debuggingEnabled) {
      LOG.info("Sending to " + address + ": " + packet);
    }
    messenger.sendToAddress(address, packet);
  }

  /**
   * Sends a JSON packet to a client.
   * 
   * @param isa
   * @param msg
   * @throws IOException
   */
  @Override
  public void sendToClient(InetSocketAddress isa, JSONObject msg) throws IOException {
    messenger.sendToAddress(isa, msg);
  }

  /**
   * Test code.
   */
  public void testCache() {
    String serviceName = "fred";
    Set<InetSocketAddress> actives;
    if ((actives = getActivesIfValid(serviceName)) != null) {
      LOG.severe("Cache should be empty!");
    }
    updateCacheEntry(serviceName, new HashSet<>(Arrays.asList(new InetSocketAddress(35000))));
    if ((actives = getActivesIfValid(serviceName)) == null) {
      LOG.severe("Cache should not be empty!");
    }
    StringBuilder cacheString = new StringBuilder();
    for (Entry<String, CacheEntry> entry : cache.asMap().entrySet()) {
      cacheString.append(entry.getKey());
      cacheString.append(" => ");
      cacheString.append(entry.getValue());
      cacheString.append("\n");
    }
    LOG.info("Cache Test: \n" + cacheString.toString());
  }

}
