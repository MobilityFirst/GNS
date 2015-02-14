/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 */
package edu.umass.cs.gns.localnameserver;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import edu.umass.cs.gns.clientsupport.Admintercessor;
import edu.umass.cs.gns.clientsupport.Intercessor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.RequestHandlerParameters;
import edu.umass.cs.gns.nio.JSONDelayEmulator;
import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.nio.JSONMessageExtractor;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.nsdesign.packet.DNSPacket;
import edu.umass.cs.gns.nsdesign.packet.NameServerLoadPacket;
import edu.umass.cs.gns.nsdesign.packet.RequestActivesPacket;
import edu.umass.cs.gns.nsdesign.packet.SelectRequestPacket;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
//import edu.umass.cs.gns.util.ConsistentHashing;
import edu.umass.cs.gns.util.GnsMessenger;
import edu.umass.cs.gns.util.MovingAverage;
import edu.umass.cs.gns.util.Util;
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
import org.json.JSONException;

/**
 * Implements basic functionality needed by servers to handle client type requests.
 * Abstracts out the storing of request info, caching and communication needs of
 * a node.
 *
 * Note: This based on LNS code, but at some point the idea was that the LNS and NS
 * could both use this interface. Not sure if that is going to happen now, but there
 * is a need for certain services at the NS that the LNS implements (like caching and
 * retransmission of lookups).
 *
 * @author westy
 */
public class BasicClientRequestHandler<NodeIDType> implements ClientRequestHandlerInterface<NodeIDType> {

  private final Intercessor intercessor;
  private final Admintercessor admintercessor;
  private final RequestHandlerParameters parameters;
  private final ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(5);
  /**
   * Map of information about queries transmitted. Key: QueryId, Value: QueryInfo (id, name, time etc.)
   */
  private final ConcurrentMap<Integer, RequestInfo> requestInfoMap;
  private final ConcurrentMap<Integer, SelectInfo> selectTransmittedMap;

  /**
   * Cache of Name records Key: Name, Value: CacheEntry (DNS_SUBTYPE_QUERY record)
   *
   */
  private final Cache<String, CacheEntry<NodeIDType>> cache;

  /**
   * Map of name record statistics *
   */
  private final ConcurrentMap<String, NameRecordStats> nameRecordStatsMap;

  /**
   * GNS node config object used by LNS to toString node information, such as IP, Port, ping latency.
   */
  private final GNSNodeConfig<NodeIDType> gnsNodeConfig;

  private final ConsistentReconfigurableNodeConfig nodeConfig;

  private final InterfaceJSONNIOTransport<NodeIDType> tcpTransport;

  private final Random random;

  /**
   * Host address of the local name server.
   */
  private final InetSocketAddress nodeAddress;

  /**
   * Instrumentation: Keep track of the number of requests coming in.
   */
  long receivedRequests = 0;

  public BasicClientRequestHandler(Intercessor intercessor, Admintercessor admintercessor,
          InetSocketAddress nodeAddress, GNSNodeConfig<NodeIDType> gnsNodeConfig,
          LNSPacketDemultiplexer demultiplexer, RequestHandlerParameters parameters) throws IOException {
    this.intercessor = intercessor;
    this.admintercessor = admintercessor;
    this.parameters = parameters;
    this.nodeAddress = nodeAddress;
    // FOR NOW WE KEEP BOTH
    this.nodeConfig = new ConsistentReconfigurableNodeConfig(gnsNodeConfig);
    this.gnsNodeConfig = gnsNodeConfig;
    this.requestInfoMap = new ConcurrentHashMap<>(10, 0.75f, 3);
    this.selectTransmittedMap = new ConcurrentHashMap<>(10, 0.75f, 3);
    this.random = new Random(System.currentTimeMillis());
    this.cache = CacheBuilder.newBuilder().concurrencyLevel(5).maximumSize(parameters.getCacheSize()).build();
    this.nameRecordStatsMap = new ConcurrentHashMap<>(16, 0.75f, 5);
    this.tcpTransport = initTransport(demultiplexer);
  }

  @SuppressWarnings("unchecked") // calls a static method
  private InterfaceJSONNIOTransport<NodeIDType> initTransport(LNSPacketDemultiplexer demultiplexer) throws IOException {
    GNS.getLogger().info("Starting LNS listener on " + nodeAddress);
    JSONNIOTransport gnsNiot = new JSONNIOTransport(nodeAddress, gnsNodeConfig, new JSONMessageExtractor(demultiplexer));
    if (parameters.isEmulatePingLatencies()) {
      JSONDelayEmulator.emulateConfigFileDelays(gnsNodeConfig, parameters.getVariation());
    }
    new Thread(gnsNiot).start();
    // id is null here because we're the LNS
    return new GnsMessenger(null, gnsNiot, executorService);
  }

  /**
   * @return the executorService
   */
  @Override
  public ScheduledThreadPoolExecutor getExecutorService() {
    return executorService;
  }

  @Override
  public GNSNodeConfig<NodeIDType> getGnsNodeConfig() {
    return gnsNodeConfig;
  }

  @Override
  public ConsistentReconfigurableNodeConfig<NodeIDType> getNodeConfig() {
    return nodeConfig;
  }

  @Override
  public InetSocketAddress getNodeAddress() {
    return nodeAddress;
  }

  @Override
  public Intercessor getIntercessor() {
    return intercessor;
  }

  @Override
  public Admintercessor getAdmintercessor() {
    return admintercessor;
  }

  @Override
  public RequestHandlerParameters getParameters() {
    return parameters;
  }

  // REQUEST INFO METHODS 
  // What happens when this overflows?
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
  public int addSelectInfo(String recordKey, SelectRequestPacket incomingPacket) {
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
   * Returns true if the local name server cache contains DNS_SUBTYPE_QUERY record for the specified name, false otherwise
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
   * Adds a new CacheEntry (NameRecord) from a DNS_SUBTYPE_QUERY packet. Overwrites existing cache entry for a name, if the name
   * record exist in the cache.
   *
   * @param packet DNS_SUBTYPE_QUERY packet containing record
   */
  @Override
  public CacheEntry<NodeIDType> addCacheEntry(DNSPacket<NodeIDType> packet) {
    CacheEntry<NodeIDType> entry = new CacheEntry<NodeIDType>(packet,
            nodeConfig.getReplicatedReconfigurators(packet.getGuid()));
    cache.put(entry.getName(), entry);
    return entry;
  }

  @Override
  public CacheEntry<NodeIDType> addCacheEntry(RequestActivesPacket<NodeIDType> packet) {
    CacheEntry<NodeIDType> entry = new CacheEntry<NodeIDType>(packet,
            nodeConfig.getReplicatedReconfigurators(packet.getName()));
    cache.put(entry.getName(), entry);
    return entry;
  }

  /**
   * Updates an existing cache entry with new information from a DNS_SUBTYPE_QUERY packet.
   *
   * @param packet DNS_SUBTYPE_QUERY packet containing record
   */
  @Override
  public CacheEntry<NodeIDType> updateCacheEntry(DNSPacket<NodeIDType> packet) {
    CacheEntry<NodeIDType> entry = cache.getIfPresent(packet.getGuid());
    if (entry == null) {
      return null;
    }
    entry.updateCacheEntry(packet);
    return entry;
  }

  @Override
  public void updateCacheEntry(RequestActivesPacket<NodeIDType> packet) {
    CacheEntry<NodeIDType> entry = cache.getIfPresent(packet.getName());
    if (entry == null) {
      return;
    }
    entry.updateCacheEntry(packet);
  }

  @Override
  public void updateCacheEntry(ConfirmUpdatePacket<NodeIDType> packet, String name, String key) {
    switch (packet.getType()) {
      case ADD_CONFIRM:
        cache.put(name, new CacheEntry<NodeIDType>(name, nodeConfig.getReplicatedReconfigurators(name)));
        //cache.put(name, new CacheEntry<NodeIDType>(name, (Set<NodeIDType>)ConsistentHashing.getReplicaControllerSet(name)));
        break;
      case REMOVE_CONFIRM:
        cache.invalidate(name);
        break;
      case UPDATE_CONFIRM:
        CacheEntry<NodeIDType> entry = cache.getIfPresent(name);
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
  public CacheEntry<NodeIDType> getCacheEntry(String name) {
    return cache.getIfPresent(name);
  }

  /**
   * Returns a Set containing name and CacheEntry
   *
   * @return
   */
  public Set<Map.Entry<String, CacheEntry<NodeIDType>>> getCacheEntrySet() {
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
    CacheEntry<NodeIDType> cacheEntry = cache.getIfPresent(name);
    return (cacheEntry != null) ? cacheEntry.isValidNameserver() : false;
  }

  @Override
  public void invalidateActiveNameServer(String name) {
    CacheEntry<NodeIDType> cacheEntry = cache.getIfPresent(name);
    if (cacheEntry != null) {
      cacheEntry.invalidateActiveNameServer();
    }
  }

  @Override
  public int timeSinceAddressCached(String name, String recordKey) {
    CacheEntry<NodeIDType> cacheEntry = cache.getIfPresent(name);
    return (cacheEntry != null) ? cacheEntry.timeSinceAddressCached(recordKey) : -1;
  }

  /**
   **
   * Prints local name server cache (and sorts it for convenience)
   */
  @Override
  public String getCacheLogString(String preamble) {
    StringBuilder cacheTable = new StringBuilder();
    List<CacheEntry<NodeIDType>> list = new ArrayList<CacheEntry<NodeIDType>>(cache.asMap().values());
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
  public Set<NodeIDType> getReplicaControllers(String name) {
    CacheEntry<NodeIDType> cacheEntry = cache.getIfPresent(name);
    return (cacheEntry != null) ? cacheEntry.getReplicaControllers() : nodeConfig.getReplicatedReconfigurators(name);
    //return (cacheEntry != null) ? cacheEntry.getReplicaControllers() : (Set<NodeIDType>)ConsistentHashing.getReplicaControllerSet(name);
  }

  /**
   **
   * Returns the closest primary name server for <i>name</i>.
   *
   * @return Closest primary name server for <i>name</i>, or -1 if no such name server is present.
   *
   */
  @Override
  public NodeIDType getClosestReplicaController(String name, Set<NodeIDType> nameServersQueried) {
    Set<NodeIDType> primaries = getReplicaControllers(name);
    if (parameters.isDebugMode()) {
      GNS.getLogger().info("Primary Name Servers: " + Util.setOfNodeIdToString(primaries) + " for name: " + name);
    }

    NodeIDType x = gnsNodeConfig.getClosestServer(primaries, nameServersQueried);
    if (parameters.isDebugMode()) {
      GNS.getLogger().info("Closest Primary Name Server: " + x.toString() + " NS Queried: " + Util.setOfNodeIdToString(nameServersQueried));
    }
    return x;
  }

  /**
   * Send packet to NS
   *
   * @param json
   * @param ns
   */
  @Override
  public void sendToNS(JSONObject json, NodeIDType ns) {
    try {
      if (parameters.isDebugMode()) {
        GNS.getLogger().info("Send to: " + ns + " json: " + json);
      }
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

  // STATS MAP
  @Override
  public NameRecordStats getStats(String name) {
    return nameRecordStatsMap.get(name);
  }

  @Override
  @SuppressWarnings("unchecked") // don't understand why there is a warning here!
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

  long lastRecordedTime = -1;
  // Maintains a moving average of server request load to smooth out the burstiness.
  private long deferedCnt = 0; // a little hair in case we are getting requests too fast for the nanosecond timer (is this likely?)
  private MovingAverage averageRequestsPerSecond = new MovingAverage(40);

  @Override
  public void updateRequestStatistics() {
    // first time do nothing
    if (lastRecordedTime == -1) {
      lastRecordedTime = System.nanoTime();
      return;
    }
    long currentTime = System.nanoTime();
    long timeDiff = currentTime - lastRecordedTime;
    deferedCnt++;
    // in case we are running faster than the clock
    if (timeDiff != 0) {
      // multiple by 1000 cuz we're computing Ops per SECOND
      averageRequestsPerSecond.add((int) (deferedCnt * 1000000000L / timeDiff));
      deferedCnt = 0;
      lastRecordedTime = currentTime;
    }
    receivedRequests++;
  }

  /**
   * Instrumentation.
   *
   * @return
   */
  @Override
  public long getReceivedRequests() {
    return receivedRequests;
  }

  /**
   * Instrumentation.
   *
   * @return
   */
  @Override
  public int getRequestsPerSecond() {
    return (int) Math.round(averageRequestsPerSecond.getAverage());
  }

  @Override
  public void handleNameServerLoadPacket(JSONObject json) throws JSONException {
    NameServerLoadPacket<NodeIDType> nsLoad = new NameServerLoadPacket<NodeIDType>(json, gnsNodeConfig);
    nameServerLoads.put(nsLoad.getReportingNodeID(), nsLoad.getLoadValue());
  }

  private ConcurrentHashMap<NodeIDType, Double> nameServerLoads;

  public ConcurrentHashMap<NodeIDType, Double> getNameServerLoads() {
    return nameServerLoads;
  }

  @Override
  public NodeIDType selectBestUsingLatencyPlusLoad(Set<NodeIDType> serverIDs) {
    if (serverIDs == null || serverIDs.size() == 0) {
      return null;
    }
    NodeIDType selectServer = null;
    // select server whose latency + load is minimum
    double selectServerLatency = Double.MAX_VALUE;
    for (NodeIDType x : serverIDs) {
      if (getGnsNodeConfig().getPingLatency(x) > 0) {
        double totallatency = 5 * nameServerLoads.get(x) + (double) getGnsNodeConfig().getPingLatency(x);
        if (totallatency < selectServerLatency) {
          selectServer = x;
          selectServerLatency = totallatency;
        }
      }
    }
    return selectServer;
  }

}
