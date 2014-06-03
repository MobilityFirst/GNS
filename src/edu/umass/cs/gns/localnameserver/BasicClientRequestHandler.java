/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.localnameserver;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.RequestHandlerParameters;
import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nio.GNSNIOTransportInterface;
import edu.umass.cs.gns.nio.JSONMessageExtractor;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.nsdesign.packet.DNSPacket;
import edu.umass.cs.gns.nsdesign.packet.RequestActivesPacket;
import edu.umass.cs.gns.nsdesign.packet.SelectRequestPacket;
import edu.umass.cs.gns.util.ConsistentHashing;
import edu.umass.cs.gns.util.GnsMessenger;
import edu.umass.cs.gns.util.NameRecordKey;
import org.json.JSONObject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Implements basic functionality needed by servers to handle client type requests.
 * Abstracts out the storing of request info, caching and communication needs of
 * a node.
 *
 * Note: This based on LNS code, but at some point the idea was that the LNS and NS
 * would both use this interface. It's not done yet (interrupted for more pressing
 * issues).
 *
 * @author westy
 */
public class BasicClientRequestHandler implements ClientRequestHandlerInterface {

  private final RequestHandlerParameters parameters;
  private final ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(5);
  /**
   * Map of information about queries transmitted. Key: QueryId, Value: QueryInfo (id, name, time etc.)
   */
  private final ConcurrentMap<Integer, RequestInfo> requestInfoMap;
  private final ConcurrentMap<Integer, SelectInfo> selectTransmittedMap;

  /**
   * Cache of Name records Key: Name, Value: CacheEntry (DNS record)
   *
   */
  private final Cache<String, CacheEntry> cache;

  /**
   * Map of name record statistics *
   */
  private final ConcurrentMap<String, NameRecordStats> nameRecordStatsMap;

  /**
   * GNS node config object used by LNS to get node information, such as IP, Port, ping latency.
   */
  private final GNSNodeConfig gnsNodeConfig;

  private final GNSNIOTransportInterface tcpTransport;

  private final Random random;

  /**
   * Name Server ID *
   */
  private final int nodeID;

  public BasicClientRequestHandler(int nodeID, GNSNodeConfig gnsNodeConfig, RequestHandlerParameters parameters) throws IOException {
    this.parameters = parameters;
    this.nodeID = nodeID;
    this.gnsNodeConfig = gnsNodeConfig;
    this.requestInfoMap = new ConcurrentHashMap<>(10, 0.75f, 3);
    this.selectTransmittedMap = new ConcurrentHashMap<>(10, 0.75f, 3);
    this.random = new Random(System.currentTimeMillis());
    this.cache = CacheBuilder.newBuilder().concurrencyLevel(5).maximumSize(parameters.getCacheSize()).build();
    this.nameRecordStatsMap = new ConcurrentHashMap<>(16, 0.75f, 5);
    this.tcpTransport = initTransport();
  }

  private GNSNIOTransportInterface initTransport() throws IOException {

    new LNSListenerUDP(gnsNodeConfig, this).start();

    GNS.getLogger().info("LNS listener started.");
    GNSNIOTransport gnsNiot = new GNSNIOTransport(nodeID, gnsNodeConfig, new JSONMessageExtractor(new LNSPacketDemultiplexer(this)));
    new Thread(gnsNiot).start();
    return new GnsMessenger(nodeID, gnsNiot, executorService);
  }

  /**
   * @return the executorService
   */
  @Override
  public ScheduledThreadPoolExecutor getExecutorService() {
    return executorService;
  }

  @Override
  public GNSNodeConfig getGnsNodeConfig() {
    return gnsNodeConfig;
  }

  @Override
  public int getNodeID() {
    return nodeID;
  }

  @Override
  public RequestHandlerParameters getParameters() {
    return parameters;
  }

  // REQUEST INFO METHODS
  private int currentRequestID = 0;

  @Override
  public synchronized int getUniqueRequestID() {
    return currentRequestID++;
  }

  @Override
  public void addRequestInfo(int id, RequestInfo requestInfo) {
    requestInfoMap.put(id, requestInfo);
  }


  @Override
  public int addSelectInfo(NameRecordKey recordKey, SelectRequestPacket incomingPacket) {
    int id;
    do {
      id = random.nextInt();
    } while (selectTransmittedMap.containsKey(id));
    //Add query info
    SelectInfo query = new SelectInfo(id);
    selectTransmittedMap.put(id, query);
    return id;
  }

  @Override
  public RequestInfo getRequestInfo(int id) {
    return requestInfoMap.get(id);
  }

  /**
   **
   * Removes and returns QueryInfo entry from the map for a query Id..
   *
   * @param id Query Id
   * @return
   */
  @Override
  public RequestInfo removeRequestInfo(int id) {
    return requestInfoMap.remove(id);
  }


  @Override
  public SelectInfo removeSelectInfo(int id) {
    return selectTransmittedMap.remove(id);
  }


  @Override
  public SelectInfo getSelectInfo(int id) {
    return selectTransmittedMap.get(id);
  }


  // CACHE METHODS
  @Override
  public void invalidateCache() {
    cache.invalidateAll();
  }

  /**
   **
   * Returns true if the local name server cache contains DNS record for the specified name, false otherwise
   *
   * @param name Host/Domain name
   */
  @Override
  public boolean containsCacheEntry(String name) {
    //return cache.getIfPresent(new NameAndRecordKey(name, recordKey)) != null;
    return cache.getIfPresent(name) != null;
  }

  /**
   **
   * Adds a new CacheEntry (NameRecord) from a DNS packet. Overwrites existing cache entry for a name, if the name
   * record exist in the cache.
   *
   * @param packet DNS packet containing record
   */
  @Override
  public CacheEntry addCacheEntry(DNSPacket packet) {
    CacheEntry entry = new CacheEntry(packet);
    cache.put(entry.getName(), entry);
    return entry;
  }

  @Override
  public CacheEntry addCacheEntry(RequestActivesPacket packet) {
    CacheEntry entry = new CacheEntry(packet);
    cache.put(entry.getName(), entry);
    return entry;
  }

  /**
   * Updates an existing cache entry with new information from a DNS packet.
   *
   * @param packet DNS packet containing record
   */
  @Override
  public CacheEntry updateCacheEntry(DNSPacket packet) {
    CacheEntry entry = cache.getIfPresent(packet.getGuid());
    if (entry == null) {
      return null;
    }
    entry.updateCacheEntry(packet);
    return entry;
  }

  @Override
  public void updateCacheEntry(RequestActivesPacket packet) {
    CacheEntry entry = cache.getIfPresent(packet.getName());
    if (entry == null) {
      return;
    }
    entry.updateCacheEntry(packet);
  }

  @Override
  public void updateCacheEntry(ConfirmUpdatePacket packet, String name, NameRecordKey key) {
    switch (packet.getType()) {
      case CONFIRM_ADD:
        // active name servers will be the same as replica controllers, so we update cache with active replica set
        RequestActivesPacket reqActives = new RequestActivesPacket(name, nodeID, 0, -1);
        reqActives.setActiveNameServers(ConsistentHashing.getReplicaControllerSet(name));
        cache.put(name, new CacheEntry(reqActives));
        break;
      case CONFIRM_REMOVE:
        cache.invalidate(name);
        break;
      case CONFIRM_UPDATE:
        CacheEntry entry = cache.getIfPresent(name);
        if (entry != null) {
          entry.updateCacheEntry(packet);
        }
        break;
    }
  }

  /**
   * Returns a cache entry for the specified name. Returns null if the cache does not have the key mapped to an entry
   *
   * @param name Host/Domain name
   */
  @Override
  public CacheEntry getCacheEntry(String name) {
    return cache.getIfPresent(name);
  }

  /**
   * Returns a Set containing name and CacheEntry
   *
   * @return
   */
  public Set<Map.Entry<String, CacheEntry>> getCacheEntrySet() {
    return cache.asMap().entrySet();
  }

  /**
   * Checks the validity of active nameserver set in cache.
   *
   * @param name Host/device/domain name whose name record is cached.
   * @return Returns true if the entry is valid, false otherwise
   */
  @Override
  public boolean isValidNameserverInCache(String name) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    return (cacheEntry != null) ? cacheEntry.isValidNameserver() : false;
  }

  @Override
  public void invalidateActiveNameServer(String name) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    if (cacheEntry != null) {
      cacheEntry.invalidateActiveNameServer();
    }
  }

  @Override
  public int timeSinceAddressCached(String name, NameRecordKey recordKey) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    return (cacheEntry != null) ? cacheEntry.timeSinceAddressCached(recordKey) : -1;
  }

  /**
   **
   * Prints local name server cache (and sorts it for convenience)
   */
  @Override
  public String getCacheLogString(String preamble) {
    StringBuilder cacheTable = new StringBuilder();
    List<CacheEntry> list = new ArrayList(cache.asMap().values());
    Collections.sort(list, new CacheComparator());
    for (CacheEntry entry : list) {
      cacheTable.append("\n");
      cacheTable.append(entry.toString());
    }
    return preamble + cacheTable.toString();
  }

  static class CacheComparator implements Comparator<CacheEntry> {

    @Override
    public int compare(CacheEntry t1, CacheEntry t2) {
      return t1.compareTo(t2);
    }
  }

  // NETWORK METHODS
  /**
   **
   * Return a Set containing ids of primary replica for <i>name</i>
   *
   * @param name
   * @return
   */
  @Override
  public Set<Integer> getReplicaControllers(String name) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    return (cacheEntry != null) ? cacheEntry.getReplicaControllers() : ConsistentHashing.getReplicaControllerSet(name);
  }

  /**
   **
   * Returns the closest primary name server for <i>name</i>.
   *
   * @return Closest primary name server for <i>name</i>, or -1 if no such name server is present.
   *
   */
  @Override
  public int getClosestReplicaController(String name, Set<Integer> nameServersQueried) {
    try {
      Set<Integer> primary = getReplicaControllers(name);
      if (parameters.isDebugMode()) GNS.getLogger().fine("Primary Name Servers: " + primary.toString() + " for name: " + name);

      int x = gnsNodeConfig.getClosestServer(primary, nameServersQueried);
      if (parameters.isDebugMode()) GNS.getLogger().fine("Closest Primary Name Server: " + x + " NS Queried: " + nameServersQueried);
      return x;
    } catch (Exception e) {
      return -1;
    }
  }

  /**
   * Send packet to NS
   *
   * @param json
   * @param ns
   */
  @Override
  public void sendToNS(JSONObject json, int ns) {
    try {
      tcpTransport.sendToID(ns, json);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * Send packet to NS
   */
  @Override
  public void sendToAddress(JSONObject json, String address, int port) {
    try {
      tcpTransport.sendToAddress(new InetSocketAddress(address, port), json);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public int getDefaultCoordinatorReplica(String name, Set<Integer> nodeIDs) {
    if (nodeIDs == null || nodeIDs.isEmpty()) {
      return -1;
    }
    Random r = new Random(name.hashCode());
    ArrayList<Integer> x1 = new ArrayList<Integer>(nodeIDs);
    Collections.sort(x1);
    Collections.shuffle(x1, r);
    for (int x : x1) {
      return x;
    }
    return x1.get(0);
    //    return  x1.get(count);
  }

  // STATS MAP
  @Override
  public NameRecordStats getStats(String name) {
    return nameRecordStatsMap.get(name);
  }

  @Override
  public Set<String> getNameRecordStatsKeySet() {
    return nameRecordStatsMap.keySet();
  }

  @Override
  public void incrementLookupRequest(String name) {

    if (nameRecordStatsMap.containsKey(name)) {
      nameRecordStatsMap.get(name).incrementLookupCount();
    } else {
      NameRecordStats nameRecordStats = new NameRecordStats();
      nameRecordStats.incrementLookupCount();
      nameRecordStatsMap.put(name, nameRecordStats);
    }
  }

  @Override
  public void incrementUpdateRequest(String name) {
    if (nameRecordStatsMap.containsKey(name)) {
      nameRecordStatsMap.get(name).incrementUpdateCount();
    } else {
      NameRecordStats nameRecordStats = new NameRecordStats();
      nameRecordStats.incrementUpdateCount();
      nameRecordStatsMap.put(name, nameRecordStats);
    }
  }

  @Override
  public void incrementLookupResponse(String name) {
    NameRecordStats nameRecordStats = nameRecordStatsMap.get(name);
    if (nameRecordStats != null) {
      nameRecordStats.incrementLookupResponse();
    }
  }

  @Override
  public void incrementUpdateResponse(String name) {
    NameRecordStats nameRecordStats = nameRecordStatsMap.get(name);
    if (nameRecordStats != null) {
      nameRecordStats.incrementUpdateResponse();
    }
  }

  /**
   **
   * Prints name record statistic
   *
   * @return
   */
  @Override
  public String getNameRecordStatsMapLogString() {
    StringBuilder str = new StringBuilder();
    for (Map.Entry<String, NameRecordStats> entry : nameRecordStatsMap.entrySet()) {
      str.append("\n");
      str.append("Name " + entry.getKey());
      str.append("->");
      str.append(" " + entry.getValue().toString());
    }
    return "***NameRecordStatsMap***" + str.toString();
  }

}
