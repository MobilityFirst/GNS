/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.localnameserver;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import static edu.umass.cs.gns.clientsupport.Defs.HELP;
import static edu.umass.cs.gns.localnameserver.LNSNodeConfig.INVALID_PING_LATENCY;
import static edu.umass.cs.gns.localnameserver.LocalNameServerOptions.DEBUG;
import static edu.umass.cs.gns.localnameserver.LocalNameServerOptions.NS_FILE;
import static edu.umass.cs.gns.localnameserver.LocalNameServerOptions.PORT;
import static edu.umass.cs.gns.localnameserver.RequestActives.log;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.nio.JSONMessageExtractor;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.Shutdownable;
import edu.umass.cs.gns.protocoltask.ProtocolExecutor;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.gns.util.NetworkUtils;
import edu.umass.cs.gns.util.ParametersAndOptions;
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

  public static final int REQUEST_ACTIVES_QUERY_TIMEOUT = 1000; // milleseconds
  public static final int MAX_QUERY_WAIT_TIME = 4000; // milleseconds
  public static final int DEFAULT_VALUE_CACHE_TTL = 10000; // milleseconds

  public static final Logger LOG = Logger.getLogger(LocalNameServer.class.getName());

  public final static int DEFAULT_LNS_TCP_PORT = 24398;

  private static final ConcurrentMap<Integer, LNSRequestInfo> outstandingRequests = new ConcurrentHashMap<>(10, 0.75f, 3);

  private final ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(5);

  private final Cache<String, CacheEntry> cache;

  private InterfaceJSONNIOTransport tcpTransport;
  private JSONMessenger messenger;
  private ProtocolExecutor protocolExecutor;
  private final LNSNodeConfig nodeConfig;
  private final LNSConsistentReconfigurableNodeConfig crNodeConfig;
  private final InetSocketAddress nodeAddress;
  private final AbstractPacketDemultiplexer demultiplexer;
  public static boolean debuggingEnabled = false;

  public LocalNameServer(InetSocketAddress nodeAddress, LNSNodeConfig nodeConfig) {
    this.nodeAddress = nodeAddress;
    this.nodeConfig = nodeConfig;
    this.crNodeConfig = new LNSConsistentReconfigurableNodeConfig(nodeConfig);
    this.demultiplexer = new LNSPacketDemultiplexer(this);
    this.cache = CacheBuilder.newBuilder().concurrencyLevel(5).maximumSize(1000).build();

    try {
      this.tcpTransport = initTransport(demultiplexer);
      messenger = new JSONMessenger<String>(tcpTransport);
      this.protocolExecutor = new ProtocolExecutor<>(messenger);
    } catch (IOException e) {
      LOG.info("Unabled to start LNS listener: " + e);
      System.exit(0);
    }
  }

  private InterfaceJSONNIOTransport initTransport(AbstractPacketDemultiplexer demultiplexer) throws IOException {
    LOG.info("Starting LNS listener on " + nodeAddress);
    JSONNIOTransport gnsNiot = new JSONNIOTransport(nodeAddress, crNodeConfig, new JSONMessageExtractor(demultiplexer));

    new Thread(gnsNiot).start();
    // id is null here because we're the LNS
    return new JSONMessenger<>(gnsNiot);
  }

  @Override
  public void shutdown() {
    messenger.stop();
    tcpTransport.stop();
    demultiplexer.stop();
    protocolExecutor.stop();
  }

  public static void main(String[] args) throws IOException {
    Map<String, String> options
            = ParametersAndOptions.getParametersAsHashMap(LocalNameServer.class.getCanonicalName(),
                    LocalNameServerOptions.getAllOptions(), args);
    if (options.containsKey(HELP)) {
      ParametersAndOptions.printUsage(LocalNameServer.class.getCanonicalName(),
              LocalNameServerOptions.getAllOptions());
      System.exit(0);
    }
    LocalNameServerOptions.initializeFromOptions(options);
    try {
      InetSocketAddress address = new InetSocketAddress(NetworkUtils.getLocalHostLANAddress().getHostAddress(),
              options.containsKey(PORT) ? Integer.parseInt(options.get(PORT)) : DEFAULT_LNS_TCP_PORT);
      LocalNameServer lns = new LocalNameServer(address, new LNSNodeConfig(options.get(NS_FILE)));
      lns.testCache();
    } catch (IOException e) {
      System.out.println("Usage: java -cp GNS.jar edu.umass.cs.gns.localnameserver <nodeConfigFile>");
    }
  }

  @Override
  public InterfaceJSONNIOTransport getTcpTransport() {
    return tcpTransport;
  }

  @Override
  public ProtocolExecutor getProtocolExecutor() {
    return protocolExecutor;
  }

  @Override
  public LNSConsistentReconfigurableNodeConfig getNodeConfig() {
    return crNodeConfig;
  }

  @Override
  public InetSocketAddress getNodeAddress() {
    return nodeAddress;
  }

  @Override
  public AbstractPacketDemultiplexer getDemultiplexer() {
    return demultiplexer;
  }

  @Override
  public boolean isDebugMode() {
    return debuggingEnabled;
  }

  @Override
  public ScheduledThreadPoolExecutor getExecutorService() {
    return executorService;
  }

  @Override
  public void addRequestInfo(int id, LNSRequestInfo requestInfo) {
    outstandingRequests.put(id, requestInfo);
  }

  @Override
  public LNSRequestInfo removeRequestInfo(int id) {
    return outstandingRequests.remove(id);
  }

  @Override
  public LNSRequestInfo getRequestInfo(int id) {
    return outstandingRequests.get(id);
  }

  /**
   * Selects the closest Name Server from a set of Name Servers.
   *
   * @param servers
   * @return id of closest server or INVALID_NAME_SERVER_ID if one can't be found
   */
  @Override
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
  @Override
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
      long pingLatency = nodeConfig.getPingLatency(serverId);
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

  // Caching
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

  @Override
  public String getValueIfValid(String name) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    if (cacheEntry != null && cacheEntry.isValidValue()) {
      return cacheEntry.getValue();
    } else {
      return null;
    }
  }

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

  @Override
  public Set<InetSocketAddress> getActivesIfValid(String name) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    if (cacheEntry != null && cacheEntry.isValidActives()) {
      return cacheEntry.getActiveNameServers();
    } else {
      return null;
    }
  }

  @Override
  public void invalidateCache() {
    cache.invalidateAll();
  }

  @Override
  public boolean containsCacheEntry(String name) {
    return cache.getIfPresent(name) != null;
  }

  @Override
  public boolean handleEvent(JSONObject json) throws JSONException {
    BasicReconfigurationPacket rcEvent
            = (BasicReconfigurationPacket) ReconfigurationPacket.getReconfigurationPacket(json, nodeConfig);
    return this.protocolExecutor.handleEvent(rcEvent);
  }

  @Override
  public void sendToClosestServer(Set<InetSocketAddress> servers, JSONObject packet) throws IOException {
    InetSocketAddress address = getClosestServer(servers);
    // Remove these so the stamper will put new ones in so the packet will find it's way back here.
    packet.remove(JSONNIOTransport.DEFAULT_IP_FIELD);
    packet.remove(JSONNIOTransport.DEFAULT_PORT_FIELD);
    if (debuggingEnabled) {
      LOG.info("Sending to " + address + ": " + packet);
    }
    getTcpTransport().sendToAddress(address, packet);
  }

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
