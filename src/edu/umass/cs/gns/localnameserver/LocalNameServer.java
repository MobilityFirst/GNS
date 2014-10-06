/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.clientsupport.Intercessor;
import edu.umass.cs.gns.localnameserver.gnamed.UdpDnsServer;
import edu.umass.cs.gns.localnameserver.httpserver.GnsHttpServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.RequestHandlerParameters;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nsdesign.Shutdownable;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.packet.ConfirmUpdatePacket;
import edu.umass.cs.gns.nsdesign.packet.DNSPacket;
import edu.umass.cs.gns.nsdesign.packet.RequestActivesPacket;
import edu.umass.cs.gns.nsdesign.packet.SelectRequestPacket;
import edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkType;
import edu.umass.cs.gns.ping.PingManager;
import edu.umass.cs.gns.test.StartExperiment;
import edu.umass.cs.gns.test.nioclient.DBClientIntercessor;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.IOException;
import java.net.BindException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This class represents the functions of a Local Name Server.
 *
 * @author abhigyan
 */
public class LocalNameServer implements Shutdownable {

  /**
   * The address of the name server. Replaces nodeId.
   */
  private static InetSocketAddress address;

  // FIXME: Future code cleanup note: The ClientRequestHandlerInterface and the IntercessorInterface
  // are closely related. Both encapsulate some functionality in the LocalNameServer that we might want to 
  // be able to abstract out (maybe to a Nameserver someday). There should be a way to combine them further.
  // One tanglible goal is to remove all references to static LocalNameServer calls in the code.
  /**
   * Implements handling of client requests, comms and caching.
   */
  private static ClientRequestHandlerInterface requestHandler;

  /**
   * A local name server forwards the final response for all requests to intercessor.
   */
  private static IntercessorInterface intercessor;

  private static ConcurrentHashMap<String, Double> nameServerLoads;

  /**
   * Ping manager object for pinging other nodes and updating ping latencies in
   */
  private static PingManager pingManager;
  
  /**
   * We keep a pointer to the gnsNodeConfig so we can shut it down.
   */
  private static GNSNodeConfig gnsNodeConfig;

  /**
   * @return the nameServerLoads
   */
  public static ConcurrentHashMap<String, Double> getNameServerLoads() {
    return nameServerLoads;
  }

  public static PingManager getPingManager() {
    return pingManager;
  }
  

  /**
   **
   * Constructs a local name server and assigns it a node id.
   *
   * @param nodeID Local Name Server Id
   * @throws IOException
   */
  public LocalNameServer(InetSocketAddress address, GNSNodeConfig gnsNodeConfig) throws IOException, InterruptedException {
    System.out.println("Log level: " + GNS.getLogger().getLevel().getName());
    // set aaddress first because constructor for BasicClientRequestHandler reads 'nodeID' value.
    this.address = address;
    // keep a copy of this so we can shut it down later
    this.gnsNodeConfig = gnsNodeConfig;
    GNS.getLogger().info("GNS Version: " + GNS.readBuildVersion());
    RequestHandlerParameters parameters = new RequestHandlerParameters(StartLocalNameServer.debuggingEnabled,
            StartLocalNameServer.experimentMode,
            StartLocalNameServer.emulatePingLatencies,
            StartLocalNameServer.variation,
            StartLocalNameServer.adaptiveTimeout,
            StartLocalNameServer.outputSampleRate,
            StartLocalNameServer.queryTimeout,
            StartLocalNameServer.maxQueryWaitTime,
            StartLocalNameServer.cacheSize,
            StartLocalNameServer.loadDependentRedirection,
            StartLocalNameServer.replicationFramework
    );
    
    GNS.getLogger().info("Parameter values: " + parameters.toString());
    requestHandler = new BasicClientRequestHandler(address, gnsNodeConfig, parameters);

    if (!parameters.isExperimentMode()) {
      // intercessor for regular GNS use
      Intercessor.init(requestHandler);
      intercessor = new Intercessor();
    } else {
      // intercessor for four simple DB operations: add, remove, write, read only.
      intercessor = new DBClientIntercessor(-1, GNS.DEFAULT_LNS_DBCLIENT_PORT,
              new LNSPacketDemultiplexer(requestHandler));
    }

    if (!parameters.isExperimentMode()) { // creates exceptions with multiple local name servers on a machine
      GnsHttpServer.runHttp();
    }

    if (!parameters.isEmulatePingLatencies()) {
      // we emulate latencies based on ping latency given in config file,
      // and do not want ping latency values to be updated by the ping module.
      GNS.getLogger().info("LNS running at " + LocalNameServer.getAddress() + " started Ping server on port " + GNS.DEFAULT_LNS_PING_PORT);
      pingManager = new PingManager(PingManager.LOCALNAMESERVERID, gnsNodeConfig);
      pingManager.startPinging();
    }

    // After starting PingManager because it accesses PingManager.
    new LNSListenerAdmin().start();

    if (parameters.getReplicationFramework() == ReplicationFrameworkType.LOCATION) {
      new NameServerVoteThread(StartLocalNameServer.voteIntervalMillis).start();
    }

    if (parameters.isExperimentMode()) {
      GNS.getLogger().info("Starting experiment ..... ");
      new StartExperiment().startMyTest(GNSNodeConfig.INVALID_NAME_SERVER_ID, StartLocalNameServer.workloadFile, StartLocalNameServer.updateTraceFile,
              requestHandler);
      // name server loads initialized.
      if (parameters.isLoadDependentRedirection()) {
        initializeNameServerLoadMonitoring();
      }
    }

    try {
      if (StartLocalNameServer.dnsGnsOnly) {
        new UdpDnsServer(Inet4Address.getByName("0.0.0.0"), 53, null).start();
      } else {
        new UdpDnsServer(Inet4Address.getByName("0.0.0.0"), 53, "8.8.8.8").start();
      }
    } catch (BindException e) {
      GNS.getLogger().warning("LNS unable to run DNS Service (needs root permissions): " + e);
    }

  }

  /**
   * Returns the host address of this LN server.
   *
   * @return
   */
  public static InetSocketAddress getAddress() {
    return address;
  }

  /**
   * Should really only be used for testing code.
   *
   * @return
   */
  public static ClientRequestHandlerInterface getRequestHandler() {
    return requestHandler;
  }

  /**
   * @return the executorService
   */
  public static ScheduledThreadPoolExecutor getExecutorService() {
    return requestHandler.getExecutorService();
  }

  public static GNSNodeConfig getGnsNodeConfig() {
    return requestHandler.getGnsNodeConfig();
  }

  public static int getUniqueRequestID() {
    return requestHandler.getUniqueRequestID();
  }

  public static void addRequestInfo(int id, RequestInfo requestInfo) {
    requestHandler.addRequestInfo(id, requestInfo);
  }

  /**
   **
   * Removes and returns QueryInfo entry from the map for a query Id..
   *
   * @param id Query Id
   * @return
   */
  public static RequestInfo removeRequestInfo(int id) {
    return requestHandler.removeRequestInfo(id);
  }

  /**
   * Returns the update info for id.
   *
   * @param id
   * @return
   */
  public static RequestInfo getRequestInfo(int id) {
    return requestHandler.getRequestInfo(id);
  }

  public static int addSelectInfo(String recordKey, SelectRequestPacket incomingPacket) {
    return requestHandler.addSelectInfo(recordKey, incomingPacket);
  }

  public static SelectInfo removeSelectInfo(int id) {
    return requestHandler.removeSelectInfo(id);
  }

  public static SelectInfo getSelectInfo(int id) {
    return requestHandler.getSelectInfo(id);
  }

  // CACHE METHODS
  public static void invalidateCache() {
    requestHandler.invalidateCache();
  }

  /**
   **
   * Returns true if the local name server cache contains DNS record for the specified name, false otherwise
   *
   * @param name Host/Domain name
   */
  public static boolean containsCacheEntry(String name) {
    return requestHandler.containsCacheEntry(name);
  }

  /**
   **
   * Adds a new CacheEntry (NameRecord) from a DNS packet. Overwrites existing cache entry for a name, if the name
   * record exist in the cache.
   *
   * @param packet DNS packet containing record
   */
  public static CacheEntry addCacheEntry(DNSPacket packet) {
    return requestHandler.addCacheEntry(packet);
  }

  public static CacheEntry addCacheEntry(RequestActivesPacket packet) {
    return requestHandler.addCacheEntry(packet);
  }

  /**
   * Updates an existing cache entry with new information from a DNS packet.
   *
   * @param packet DNS packet containing record
   */
  public static CacheEntry updateCacheEntry(DNSPacket packet) {
    return requestHandler.updateCacheEntry(packet);
  }

  public static void updateCacheEntry(RequestActivesPacket packet) {
    requestHandler.updateCacheEntry(packet);
  }

  public static void updateCacheEntry(ConfirmUpdatePacket packet, String name, String key) {
    requestHandler.updateCacheEntry(packet, name, key);
  }

  /**
   * Returns a cache entry for the specified name. Returns null if the cache does not have the key mapped to an entry
   *
   * @param name Host/Domain name
   */
  public static CacheEntry getCacheEntry(String name) {
    return requestHandler.getCacheEntry(name);
  }

  /**
   * Invalidates the active name server set in cache by setting its value to <i>null</i>.
   *
   * @param name
   */
  public static void invalidateActiveNameServer(String name) {
    requestHandler.invalidateActiveNameServer(name);
  }

  /**
   * Checks the validity of active nameserver set in cache.
   *
   * @param name Host/device/domain name whose name record is cached.
   * @return Returns true if the entry is valid, false otherwise
   */
  public static boolean isValidNameserverInCache(String name) {
    return requestHandler.isValidNameserverInCache(name);
  }

  public static int timeSinceAddressCached(String name, String recordKey) {
    return requestHandler.timeSinceAddressCached(name, recordKey);
  }

  // LOCATING REPLICA CONTROLLERS
  /**
   **
   * Return a Set containing ids of primary replica for <i>name</i>
   *
   * @param name
   * @return
   */
  public static Set getReplicaControllers(String name) {
    return requestHandler.getReplicaControllers(name);
  }

  /**
   **
   * Returns the closest primary name server for <i>name</i>.
   *
   * @return Closest primary name server for <i>name</i>, or -1 if no such name server is present.
   *
   */
  public static Object getClosestReplicaController(String name, Set nameServersQueried) {
    return requestHandler.getClosestReplicaController(name, nameServersQueried);
  }

  // SENDING
  /**
   * Send packet to NS after all packet
   *
   * @param json
   * @param ns
   */
  public static void sendToNS(JSONObject json, Object ns) {
    requestHandler.sendToNS(json, ns);
  }

  /**
   * *******************END: methods for monitoring load at name servers. *******************************
   */
  /**
   * ************************************************************
   * Returns closest server including ping-latency and server-load.
   *
   * @return Best name server among serverIDs given.
   * ***********************************************************
   */
  public static String selectBestUsingLatencyPlusLoad(Set<String> serverIDs) {
    if (serverIDs == null || serverIDs.size() == 0) {
      return GNSNodeConfig.INVALID_NAME_SERVER_ID;
    }
    String selectServer = GNSNodeConfig.INVALID_NAME_SERVER_ID;
    // select server whose latency + load is minimum
    double selectServerLatency = Double.MAX_VALUE;
    for (String x : serverIDs) {
      if (requestHandler.getGnsNodeConfig().getPingLatency(x) > 0) {
        double totallatency = 5 * getNameServerLoads().get(x) + (double) requestHandler.getGnsNodeConfig().getPingLatency(x);
        if (totallatency < selectServerLatency) {
          selectServer = x;
          selectServerLatency = totallatency;
        }
      }
    }
    return selectServer;
  }

  public static int getDefaultCoordinatorReplica(String name, Set<Integer> nodeIDs) {
    return requestHandler.getDefaultCoordinatorReplica(name, nodeIDs);
  }

  /**
   * Prints local name server cache (and sorts it for convenience)
   */
  public static String getCacheLogString(String preamble) {
    return requestHandler.getCacheLogString(preamble);
  }

  // STATS MAP
  public static NameRecordStats getStats(String name) {
    return requestHandler.getStats(name);
  }

  public static Set<String> getNameRecordStatsKeySet() {
    return requestHandler.getNameRecordStatsKeySet();
  }

  public static void incrementLookupRequest(String name) {
    requestHandler.incrementLookupRequest(name);
  }

  public static void incrementUpdateRequest(String name) {
    requestHandler.incrementUpdateRequest(name);
  }

  public static void incrementLookupResponse(String name) {
    requestHandler.incrementLookupResponse(name);
  }

  public static void incrementUpdateResponse(String name) {
    requestHandler.incrementUpdateResponse(name);
  }

  /**
   **
   * Prints name record statistic
   *
   */
  public static String getNameRecordStatsMapLogString() {
    return requestHandler.getNameRecordStatsMapLogString();
  }

  public static IntercessorInterface getIntercessor() {
    return intercessor;
  }

  // MONITOR NAME SERVER LOADS
  private void initializeNameServerLoadMonitoring() {
    nameServerLoads = new ConcurrentHashMap<String, Double>();
    Set<String> nameServerIDs = requestHandler.getGnsNodeConfig().getNodeIDs();
    for (String x : nameServerIDs) {
      nameServerLoads.put(x, 0.0);
    }
    Random r = new Random();
    for (String x : nameServerIDs) {
      SendLoadMonitorPacketTask loadMonitorTask = new SendLoadMonitorPacketTask(x);
      long interval = StartLocalNameServer.nameServerLoadMonitorIntervalSeconds * 1000;
      // Query NS at different times to avoid synchronization among local name servers.
      // synchronization may cause oscillations in name server loads.
      long offset = (long) (r.nextDouble() * interval);
      requestHandler.getExecutorService().scheduleAtFixedRate(loadMonitorTask, offset, interval, TimeUnit.MILLISECONDS);
    }
  }

  public static void handleNameServerLoadPacket(JSONObject json) throws JSONException {
//    NameServerLoadPacket nsLoad = new NameServerLoadPacket(json);
//    LocalNameServer.nameServerLoads.put(nsLoad.getReportingNodeID(), nsLoad.getLoadValue());
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdown() {
    this.gnsNodeConfig.shutdown();
  }

}
