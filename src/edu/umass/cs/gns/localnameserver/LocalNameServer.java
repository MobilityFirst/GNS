/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.clientsupport.Intercessor;
import edu.umass.cs.gns.httpserver.GnsHttpServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.RequestHandlerParameters;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkType;
import edu.umass.cs.gns.ping.PingManager;
import edu.umass.cs.gns.ping.PingServer;
import edu.umass.cs.gns.test.StartExperiment;
import edu.umass.cs.gns.util.NameRecordKey;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 **
 * This class represents the functions of a Local Name Server.
 *
 * @author abhigyan
 */
public class LocalNameServer {

  // Implements handling of client requests, comms and caching
  private static ClientRequestHandlerInterface requestHandler;

  /**
   * Local Name Server ID *
   */
  private static int nodeID;

  /**
   * Unique and random query ID *
   */
//  private static Random random;
//
//  private static GNSNIOTransportInterface tcpTransport;

  private static ConcurrentHashMap<Integer, Double> nameServerLoads;

  /**
   * Ping manager object for pinging other nodes and updating ping latencies in
   */
  private static PingManager pingManager;

  /**
   * @return the nameServerLoads
   */
  public static ConcurrentHashMap<Integer, Double> getNameServerLoads() {
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
  public LocalNameServer(int nodeID, GNSNodeConfig gnsNodeConfig) throws IOException, InterruptedException {
    System.out.println("Log level: " + GNS.getLogger().getLevel().getName());
    // set node ID first because constructor for BasicClientRequestHandler reads 'nodeID' value.
    LocalNameServer.nodeID = nodeID;
    GNS.getLogger().info("GNS Version: " + GNS.readBuildVersion());
    RequestHandlerParameters parameters = new RequestHandlerParameters(StartLocalNameServer.debugMode,
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
    requestHandler = new BasicClientRequestHandler(nodeID, gnsNodeConfig, parameters);
    Intercessor.init(requestHandler);
    //startTransport();
    if (!parameters.isExperimentMode()) { // creates exceptions with multiple local name servers on a machine
      GnsHttpServer.runHttp(nodeID);
    }
    if (!parameters.isEmulatePingLatencies()) {
      // we emulate latencies based on ping latency given in config file,
      // and do not want ping latency values to be updated by the ping module.
      PingServer.startServerThread(nodeID, gnsNodeConfig);
      GNS.getLogger().info("LNS Node " + LocalNameServer.getNodeID() + " started Ping server on port " + gnsNodeConfig.getPingPort(nodeID));
      pingManager = new PingManager(nodeID, gnsNodeConfig);
      pingManager.startPinging();
    }

    // After starting PingManager because it accesses PingManager.
    new LNSListenerAdmin().start();

    if (parameters.getReplicationFramework() == ReplicationFrameworkType.LOCATION) {
      // todo commented this because locality-based replication is still under testing
      new NameServerVoteThread(StartLocalNameServer.voteIntervalMillis).start();
    }
    if (parameters.isExperimentMode()) {
      GNS.getLogger().info("Starting experiment ..... ");
      new StartExperiment().startMyTest(nodeID, StartLocalNameServer.workloadFile, StartLocalNameServer.updateTraceFile,
              requestHandler);
      // name server loads initialized.
      if (parameters.isLoadDependentRedirection()) {
        initializeNameServerLoadMonitoring();
      }
    }
  }

  /**
   * @return the nodeID
   */
  public static int getNodeID() {
    return nodeID;
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

  public static void addRequestInfo(int id, RequestInfo requestInfo){
    requestHandler.addRequestInfo(id, requestInfo);
  }

  /**
   **
   * Removes and returns QueryInfo entry from the map for a query Id..
   *
   * @param id Query Id
   * @return
   */
  public static RequestInfo removeRequestInfo(int id){
    return requestHandler.removeRequestInfo(id);
  }


  /**
   * Returns the update info for id.
   * @param id
   * @return
   */
  public static RequestInfo getRequestInfo(int id){
    return requestHandler.getRequestInfo(id);
  }

  public static int addSelectInfo(NameRecordKey recordKey, SelectRequestPacket incomingPacket) {
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

  public static void updateCacheEntry(ConfirmUpdatePacket packet, String name, NameRecordKey key) {
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

  public static int timeSinceAddressCached(String name, NameRecordKey recordKey) {
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
  public static Set<Integer> getReplicaControllers(String name) {
    return requestHandler.getReplicaControllers(name);
  }

  /**
   **
   * Returns the closest primary name server for <i>name</i>.
   *
   * @return Closest primary name server for <i>name</i>, or -1 if no such name server is present.
   *
   */
  public static int getClosestReplicaController(String name, Set<Integer> nameServersQueried) {
    return requestHandler.getClosestReplicaController(name, nameServersQueried);
  }

  // SENDING
  /**
   * Send packet to NS after all packet
   *
   * @param json
   * @param ns
   */
  public static void sendToNS(JSONObject json, int ns) {
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
  public static int selectBestUsingLatencyPlusLoad(Set<Integer> serverIDs) {
    if (serverIDs == null || serverIDs.size() == 0) {
      return -1;
    }
    int selectServer = -1;
    // select server whose latency + load is minimum
    double selectServerLatency = Double.MAX_VALUE;
    for (int x : serverIDs) {
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
          
  // MONITOR NAME SERVER LOADS
  private void initializeNameServerLoadMonitoring() {
    nameServerLoads = new ConcurrentHashMap<Integer, Double>();
    Set<Integer> nameServerIDs = requestHandler.getGnsNodeConfig().getNameServerIDs();
    for (int x : nameServerIDs) {
      nameServerLoads.put(x, 0.0);
    }
    Random r = new Random();
    for (int x : nameServerIDs) {
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

}
