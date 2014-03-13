/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import edu.umass.cs.gns.httpserver.GnsHttpServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.ReplicationFrameworkType;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.GNSNodeConfig;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nio.*;
import edu.umass.cs.gns.packet.*;
import edu.umass.cs.gns.ping.PingManager;
import edu.umass.cs.gns.ping.PingServer;
import edu.umass.cs.gns.test.TraceRequestGenerator;
import edu.umass.cs.gns.util.BestServerSelection;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.ConsistentHashing;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 **
 * This class represents the functions of a Local Name Server.
 *
 */
public class LocalNameServer {

  private static ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(5);
  /**
   * Local Name Server ID *
   */
  private static int nodeID;
  /**
   * Map of information about queries transmitted. Key: QueryId, Value: QueryInfo (id, name, time etc.)
   *
   */
  private static ConcurrentMap<Integer, DNSRequestInfo> requestTransmittedMap;
  private static ConcurrentMap<Integer, UpdateInfo> updateTransmittedMap;
  private static ConcurrentMap<Integer, SelectInfo> selectTransmittedMap;
  /**
   * Cache of Name records Key: Name, Value: CacheEntry (DNS record)
   *
   */
  private static Cache<String, CacheEntry> cache;
  /**
   * Map of name record statistic *
   */
  private static ConcurrentMap<String, NameRecordStats> nameRecordStatsMap;
  /**
   * Unique and random query ID *
   */
  private static Random random;
  private static GNSNIOTransportInterface tcpTransport; // Abhigyan: keeping this here because we are testing with GNSNIOTransport


  private static ConcurrentHashMap<Integer, Double> nameServerLoads;

  /**
   * @return the executorService
   */
  public static ScheduledThreadPoolExecutor getExecutorService() {
    return executorService;
  }

  /**
   * @return the nodeID
   */
  public static int getNodeID() {
    return nodeID;
  }

  /**
   * @return the nameServerLoads
   */
  public static ConcurrentHashMap<Integer, Double> getNameServerLoads() {
    return nameServerLoads;
  }

  /**
   **
   * Constructs a local name server and assigns it a node id.
   *
   * @param nodeID Local Name Server Id
   * @throws IOException
   */
  public LocalNameServer(int nodeID) throws IOException {
    GNS.getLogger().info("GNS Version: " + GNS.readBuildVersion());
    LocalNameServer.nodeID = nodeID;

    requestTransmittedMap = new ConcurrentHashMap<Integer, DNSRequestInfo>(10, 0.75f, 3);
    updateTransmittedMap = new ConcurrentHashMap<Integer, UpdateInfo>(10, 0.75f, 3);
    selectTransmittedMap = new ConcurrentHashMap<Integer, SelectInfo>(10, 0.75f, 3);

    random = new Random(System.currentTimeMillis());

    cache = CacheBuilder.newBuilder().concurrencyLevel(5).maximumSize(StartLocalNameServer.cacheSize).build();
    nameRecordStatsMap = new ConcurrentHashMap<String, NameRecordStats>(16, 0.75f, 5);
  }

  /**
   **
   * Starts the Local Name Server.
   *
   * @throws Exception
   */
  public void run() throws Exception {
    System.out.println("Log level: " + GNS.getLogger().getLevel().getName());


    // name server loads initialized.
    if (StartLocalNameServer.loadDependentRedirection) {
      initializeNameServerLoadMonitoring();
    }


    new LNSListenerUDP().start();

    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("LNS listener started.");
    }

    if (StartLocalNameServer.useGNSNIOTransport) {
      // Abhigyan: Keeping this code here as we are testing with GNSNIOTransport
      tcpTransport = new GNSNIOTransport(LocalNameServer.nodeID, new GNSNodeConfig(), new JSONMessageWorker(new LNSPacketDemultiplexer()));
    } else {
      tcpTransport = new NioServer(LocalNameServer.nodeID, new ByteStreamToJSONObjects(new LNSPacketDemultiplexer()), new GNSNodeConfig());
    }

    if (StartLocalNameServer.experimentMode) {
      long initialExpDelayMillis = 40000;
      try {
        Thread.sleep(initialExpDelayMillis); // Abhigyan: When multiple LNS are running on same machine, we wait for
        // all lns's to bind to their respective listening port before sending any traffic. Otherwise, another LNS could
        // start a new connection and bind to this LNS's listening port. We have seen this very often in cluster tests.
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    new Thread(tcpTransport).start();

    new LNSListenerAdmin().start();

    PingServer.startServerThread(nodeID);
    GNS.getLogger().info("LNS Node " + LocalNameServer.getNodeID() + " started Ping server on port " + ConfigFileInfo.getPingPort(nodeID));
    PingManager.startPinging(nodeID);

    //Periodically send nameserver votes for location based replication

//          if (StartLocalNameServer.experimentMode) {
//            runSimpleTest();
//          }

    if (StartLocalNameServer.experimentMode) {
      TraceRequestGenerator.generateLookupsUpdates(StartLocalNameServer.lookupTraceFile,
              StartLocalNameServer.updateTraceFile, StartLocalNameServer.lookupRate,
              StartLocalNameServer.updateRateRegular, executorService);
    }


    if (StartLocalNameServer.replicationFramework == ReplicationFrameworkType.LOCATION) {
      new NameServerVoteThread(StartLocalNameServer.voteIntervalMillis).start();
    }

    if (StartLocalNameServer.experimentMode == false) {
      GnsHttpServer.runHttp(LocalNameServer.nodeID);
    }
  }

  /********************** BEGIN: methods for read/write to info about reads (queries) and updates ****************/
  /**
   **
   * Adds information of a transmitted query to a query transmitted map.
   *
   * @param name Host/Domain name
   * @param nameserverID Name server Id
   * @param time System time during transmission
   * @return A unique id for the query
   */
  public static int addDNSRequestInfo(String name, NameRecordKey recordKey,
                                      int nameserverID, long time, String queryStatus, int lookupNumber,
                                      DNSPacket incomingPacket, int numRestarts) {
    int id;
    //Generate unique id for the query
    do {
      id = random.nextInt();
    } while (requestTransmittedMap.containsKey(id));

    //Add query info
    DNSRequestInfo query = new DNSRequestInfo(id, name, recordKey, time,
            nameserverID, queryStatus, lookupNumber,
            incomingPacket, numRestarts);
    requestTransmittedMap.put(id, query);
    return id;
  }

  public static int addUpdateInfo(String name, int nameserverID, long time,
                                  int numRestarts, UpdateAddressPacket updateAddressPacket) {
    int id;
    //Generate unique id for the query
    do {
      id = random.nextInt();
    } while (updateTransmittedMap.containsKey(id));

    //Add update info
    UpdateInfo update = new UpdateInfo(id, name, time, nameserverID, updateAddressPacket, numRestarts);
    updateTransmittedMap.put(id, update);
    return id;
  }

  public static int addSelectInfo(NameRecordKey recordKey, SelectRequestPacket incomingPacket) {
    int id;
    do {
      id = random.nextInt();
    } while (requestTransmittedMap.containsKey(id));

    //Add query info
    SelectInfo query = new SelectInfo(id);
    selectTransmittedMap.put(id, query);
    return id;
  }

  /**
   **
   * Removes and returns QueryInfo entry from the map for a query Id..
   *
   * @param id Query Id
   */
  public static DNSRequestInfo removeDNSRequestInfo(int id) {
    return requestTransmittedMap.remove(id);
  }

  public static UpdateInfo removeUpdateInfo(int id) {
    return updateTransmittedMap.remove(id);
  }

  public static SelectInfo removeSelectInfo(int id) {
    return selectTransmittedMap.remove(id);
  }

  public static UpdateInfo getUpdateInfo(int id) {
    return updateTransmittedMap.get(id);
  }

  public static SelectInfo getSelectInfo(int id) {
    return selectTransmittedMap.get(id);
  }

  /**
   **
   * Returns true if the map contains the specified query id, false otherwise.
   *
   * @param id Query Id
   */
  public static boolean containsDNSRequestInfo(int id) {
    return requestTransmittedMap.containsKey(id);
  }

  public static DNSRequestInfo getDNSRequestInfo(int id) {
    return requestTransmittedMap.get(id);
  }

  /**
   **
   * Prints information about transmitted queries that have not received a response.
   *
   */
  public static String dnsRequestInfoLogString(String preamble) {
    StringBuilder queryTable = new StringBuilder();
    for (DNSRequestInfo info : LocalNameServer.requestTransmittedMap.values()) {
      queryTable.append("\n");
      queryTable.append(info.toString());
    }
    return preamble + queryTable.toString();
  }

  /********************** END: methods for read/write to info about queries (read) and updates ****************/

  /********************** BEGIN: methods for read/write to the stats map *******************/
  public static NameRecordStats getStats(String name) {

    NameRecordStats nameRecordStats = nameRecordStatsMap.get(name);
    return nameRecordStats;
//    int vote = (nameRecordStats != null) ? nameRecordStats.getVotes() : 0;
//    return vote;
  }

  public static Set<String> getNameRecordStatsKeySet() {
    return nameRecordStatsMap.keySet();
  }

  public static void incrementLookupRequest(String name) {

    if (nameRecordStatsMap.containsKey(name)) {
      nameRecordStatsMap.get(name).incrementLookupCount();
    } else {
      NameRecordStats nameRecordStats = new NameRecordStats();
      nameRecordStats.incrementLookupCount();
      nameRecordStatsMap.put(name, nameRecordStats);
    }
  }

  public static void incrementUpdateRequest(String name) {
    if (nameRecordStatsMap.containsKey(name)) {
      nameRecordStatsMap.get(name).incrementUpdateCount();
    } else {
      NameRecordStats nameRecordStats = new NameRecordStats();
      nameRecordStats.incrementUpdateCount();
      nameRecordStatsMap.put(name, nameRecordStats);
    }
  }

  public static void incrementLookupResponse(String name) {
//    NameAndRecordKey nameAndType = new NameAndRecordKey(name, recordKey);
//    NameRecordStats nameRecordStats = nameRecordStatsMap.get(nameAndType);
//    if (nameRecordStats != null) {
//      nameRecordStats.incrementLookupResponse();
//    }
    NameRecordStats nameRecordStats = nameRecordStatsMap.get(name);
    if (nameRecordStats != null) {
      nameRecordStats.incrementLookupResponse();
    }
  }

  public static void incrementUpdateResponse(String name) {
//    NameAndRecordKey nameAndType = new NameAndRecordKey(name, recordKey);
//    NameRecordStats nameRecordStats = nameRecordStatsMap.get(nameAndType);
//    if (nameRecordStats != null) {
//      nameRecordStats.incrementUpdateResponse();
//    }
    NameRecordStats nameRecordStats = nameRecordStatsMap.get(name);
    if (nameRecordStats != null) {
      nameRecordStats.incrementUpdateResponse();
    }
  }

  /**
   **
   * Prints name record statistic
   *
   */
  public static String nameRecordStatsMapLogString() {
    StringBuilder str = new StringBuilder();
    for (Map.Entry<String, NameRecordStats> entry : nameRecordStatsMap.entrySet()) {
      str.append("\n");
      str.append("Name " + entry.getKey());
      str.append("->");
      str.append(" " + entry.getValue().toString());
    }
    return "***NameRecordStatsMap***" + str.toString();
  }

  /********************** END: methods for read/write to the stats map *******************/
  /********************** BEGIN: methods that read/write to the cache at the local name server ****************/
  public static void invalidateCache() {
    cache.invalidateAll();
  }

  /**
   **
   * Returns true if the local name server cache contains DNS record for the specified name, false otherwise
   *
   * @param name Host/Domain name
   */
  public static boolean containsCacheEntry(String name) {
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
  public static CacheEntry addCacheEntry(DNSPacket packet) {
    CacheEntry entry = new CacheEntry(packet);
    cache.put(entry.getName(), entry);
    return entry;
  }

  public static CacheEntry addCacheEntry(RequestActivesPacket packet) {
    CacheEntry entry = new CacheEntry(packet);
    cache.put(entry.getName(), entry);
    return entry;
  }

  /**
   * Updates an existing cache entry with new information from a DNS packet.
   *
   * @param packet DNS packet containing record
   */
  public static CacheEntry updateCacheEntry(DNSPacket packet) {
    CacheEntry entry = cache.getIfPresent(packet.getGuid());
    if (entry == null) {
      return null;
    }
    entry.updateCacheEntry(packet);
    return entry;
  }

  public static void updateCacheEntry(RequestActivesPacket packet) {
    CacheEntry entry = cache.getIfPresent(packet.getName());
    if (entry == null) {
      return;
    }
    entry.updateCacheEntry(packet);
  }

  public static void updateCacheEntry(ConfirmUpdateLNSPacket packet, String name, NameRecordKey key) {
    switch (packet.getType()) {
      case CONFIRM_ADD_LNS:
        // screw it.. let the next query generate the cache
        break;
      case CONFIRM_REMOVE_LNS:
        cache.invalidate(name);
        break;
      case CONFIRM_UPDATE_LNS:
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
  public static CacheEntry getCacheEntry(String name) {
    return cache.getIfPresent(name);
  }

  /**
   * Returns a Set containing name and CacheEntry 
   */
  public static Set<Map.Entry<String, CacheEntry>> getCacheEntrySet() {
    return cache.asMap().entrySet();
  }

  /**
   * Checks the validity of active nameserver set in cache.
   *
   * @param name Host/device/domain name whose name record is cached.
   * @return Returns true if the entry is valid, false otherwise
   */
  public static boolean isValidNameserverInCache(String name) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    return (cacheEntry != null) ? cacheEntry.isValidNameserver() : false;
  }

  public static int timeSinceAddressCached(String name, NameRecordKey recordKey) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    return (cacheEntry != null) ? cacheEntry.timeSinceAddressCached(recordKey) : -1;
  }

  /**
   * Invalidates the active name server set in cache by setting its value to <i>null</i>.
   *
   * @param name
   */
  public static void invalidateActiveNameServer(String name) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    if (cacheEntry != null) {
      cacheEntry.invalidateActiveNameServer();
    }
  }

  /**
   **
   * Return a Set containing ids of primary replica for <i>name</i>
   *
   * @param name
   * @return
   * @throws UnsupportedEncodingException
   * @throws NoSuchAlgorithmException
   */
  public static Set<Integer> getPrimaryNameServers(String name) {
    //NameAndRecordKey nameAndType = new NameAndRecordKey(name, recordKey);
    //CacheEntry cacheEntry = cache.getIfPresent(nameAndType);
    CacheEntry cacheEntry = cache.getIfPresent(name);
    //if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("GNRS Logger: Current cache entry: " + cacheEntry + " Name = " + name + " record " + recordKey);
    return (cacheEntry != null) ? cacheEntry.getPrimaryNameServer() : ConsistentHashing.getReplicaControllerSet(name);
  }

  /**
   **
   * Returns the closest primary name server for <i>name</i>.
   *
   * @return Closest primary name server for <i>name</i>, or -1 if no such name server is present.
   *
   */
  public static int getClosestPrimaryNameServer(String name, Set<Integer> nameServersQueried) {
    try {
      Set<Integer> primary = getPrimaryNameServers(name);
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("Primary Name Servers: " + primary.toString() + " for name: " + name);
      }
      int x = BestServerSelection.getSmallestLatencyNS(primary, nameServersQueried);
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("Closest Primary Name Server: " + x + " NS Queried: " + nameServersQueried);
      }
      return x;
    } catch (Exception e) {
      return -1;
    }
  }

  /**
   **
   * Generates and stores lookup query statistic with the given parameters.
   *
   * @param name Host/domain/device name
   * @param latency Latency for lookup query
   * @param numTransmission Number of transmissions
   * @param activeNameServerId Active name server id
   * @param timestamp Response timestamp
   * @return A tab seperated lookup query stats with the following format <i>Name Latency(ms) PingLatency(ms)
   * NumTransmissions ActiveNameServerId LocalNameServerId ResponseTimeStamp</i>
   *
   */
//	public static String addLookupStats(String name, NameRecordKey recordKey, long latency,
//			int numTransmission, int activeNameServerId, long timestamp, String queryStatus,
//			String nameServerQueried, String nameServerQueriedPingLatency, Set<Integer> activeNameServerSet,
//			Set<Integer> primaryNameServerSet, int lookupNumber) {
//		//Response Information: Time(ms) ActiveNS Ping(ms) Name NumTransmission LNS Timestamp(systime)
//		StringBuilder str = new StringBuilder();
//		str.append("Success-LookupRequest\t");
//		str.append(lookupNumber + "\t");
//		str.append(recordKey + "\t");
//		str.append(name);
//		str.append("\t" + latency);
//		if (queryStatus == QueryInfo.CACHE) {
//			str.append("\tNA");
//		} else {
//			str.append("\t" + ConfigFileInfo.getPingLatency(activeNameServerId));
//		}
//		str.append("\t" + ConfigFileInfo.getPingLatency(ConfigFileInfo.getClosestNameServer()));
//		str.append("\t" + ConfigFileInfo.getClosestNameServer());
//		str.append("\t" + numTransmission);
//		str.append("\t" + activeNameServerId);
//		str.append("\t" + nodeID);
//		str.append("\t" + timestamp);
//		str.append("\t" + queryStatus);
//		str.append("\t" + nameServerQueried);
//		str.append("\t" + nameServerQueriedPingLatency);
//		if (activeNameServerSet != null) {
//			str.append("\t" + activeNameServerSet.size());
//			str.append("\t" + activeNameServerSet.toString());
//		} else {
//			str.append("\t0");
//			str.append("\t[]");
//		}
//
//		if (primaryNameServerSet != null) {
//			str.append("\t" + primaryNameServerSet.toString());
//		} else {
//			str.append("\t" + "[]");
//		}
//
//		//save response time
//		String stats = str.toString();
//		//		responseTime.add( stats );
//		return stats;
//	}
  /**
   **
   * Prints local name server cache (and sorts it for convenience)
   */
  public static String cacheLogString(String preamble) {
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

  /********************** END: methods that read/write to the cache at the local name server ****************/

  /*********************BEGIN: methods for sending packets to name servers. ********************************/
  /**
   * Send packet to NS after all packet
   * @param json
   * @param ns
   */
  public static void sendToNS(JSONObject json, int ns) {

    if (StartLocalNameServer.emulatePingLatencies) { // during testing, this option is used to simulate artificial latency between lns and ns
      // packets from LNS to NS will be delayed by twice the one-way latency because we do not have data to emulate
      // latency on the reverse path from NS to LNS.
      double latency = (ConfigFileInfo.getPingLatency(ns) * (1 + random.nextDouble() * StartLocalNameServer.variation))/2;
      long timerDelay = (long) latency;
      LocalNameServer.executorService.schedule(new SendMessageWithDelay(json, ns), timerDelay, TimeUnit.MILLISECONDS);
    } else {
      sendToNSActual(json, ns);
    }
  }

  static void sendToNSActual(JSONObject json, int ns) {
//    if (json.toString().length() < 1000) {
//      try {
//        LNSListenerUDP.udpTransport.sendPacket(json, ns, GNS.PortType.NS_UDP_PORT);
//      } catch (JSONException e) {
//        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//      }
//    } else { // for large packets,  use TCP
    try {
      tcpTransport.sendToID(ns, json);
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
//    }
  }

  /*********************END: methods for sending packets to name servers. ********************************/

  /*********************BEGIN: methods for monitoring load at name servers. ********************************/
  private void initializeNameServerLoadMonitoring() {
    nameServerLoads = new ConcurrentHashMap<Integer, Double>();
    Set<Integer> nameServerIDs = ConfigFileInfo.getAllNameServerIDs();
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
      LocalNameServer.executorService.scheduleAtFixedRate(loadMonitorTask, offset, interval, TimeUnit.MILLISECONDS);
    }
  }

  public static void handleNameServerLoadPacket(JSONObject json) throws JSONException {
    NameServerLoadPacket nsLoad = new NameServerLoadPacket(json);
    LocalNameServer.nameServerLoads.put(nsLoad.getNsID(), nsLoad.getLoadValue());
  }

  /*********************END: methods for monitoring load at name servers. ********************************/

  public static int getDefaultCoordinatorReplica(String name, Set<Integer> nodeIDs) {

//    int nodeProduct = 1;
//    for (int x: nodeIDs) {
//      nodeProduct =  nodeProduct*x;
//    }
    if (nodeIDs == null || nodeIDs.size() == 0) {
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
  /**
   * Test *
   */
  // public static void main(String[] args) throws IOException {
//  	ArrayList<Integer> nameServers = new ArrayList<Integer>();
//
//  	nameServers.add(10);
//  	nameServers.add(5);
//  	nameServers.add(15);
//  	HashSet<Integer> nameServersQueried = new HashSet<Integer>();
//  	nameServersQueried.add(15);
//  	System.out.println(LocalNameServer.beehiveNSChoose(4, nameServers, nameServersQueried ));
//  	System.out.println(1423);
  //		ConfigFileInfo.readNameServerInfoLocal( "ns3", 3 );
//    LocalNameServer.cache = CacheBuilder.newBuilder().concurrencyLevel(5).maximumSize(100).build();
//    Header header = new Header(1100, 1, DNSRecordType.RCODE_NO_ERROR);
//
//    DNSPacket packet = new DNSPacket(header, "fred", NameRecordKey.EdgeRecord);
//    packet.rdata = new QueryResultValue(Arrays.asList("barney"));
//    packet.primaryNameServers = ImmutableSet.copyOf(new HashSet<Integer>(Arrays.asList(1, 2, 3)));
//    packet.activeNameServers = new HashSet<Integer>(Arrays.asList(2, 4));
//    packet.ttlAddress = 3;
//
//    LocalNameServer.addCacheEntry(packet);
//    System.out.println(cacheLogString("***CACHE:*** "));
//
//    CacheEntry cacheEntry = cache.getIfPresent(new NameAndRecordKey("fred", NameRecordKey.EdgeRecord));
//    System.out.println("\ncacheEntry: " + cacheEntry);
//    DNSPacket packet = new DNSPacket(header, "fred", NameRecordKey.EdgeRecord);
//    packet.setRdata(new QueryResultValue(Arrays.asList("barney")));
//    packet.setPrimaryNameServers(new HashSet<Integer>(Arrays.asList(1, 2, 3)));
//    packet.setActiveNameServers(new HashSet<Integer>(Arrays.asList(2, 4)));
//    packet.setTTL(3);
//
//    LocalNameServer.addCacheEntry(packet);
//    System.out.println(cacheLogString("***CACHE:*** "));
//
//    CacheEntry cacheEntry = cache.getIfPresent("fred");
//    System.out.println("\ncacheEntry: " + cacheEntry);
//    
  //cacheEntry.setValue(null);
  //System.out.println("\ncacheEntry: " + cacheEntry);
  //
  //		Set<Integer> nameserverQueried = new HashSet<Integer>();
  //		int nameserver = LocalNameServer.getClosestActiveNSFromCache( "h.com", nameserverQueried );
  //		System.out.println( "\nNameServer: " + nameserver );
  //
  //		nameserverQueried.add(2);
  //		nameserver = LocalNameServer.getClosestActiveNSFromCache( "h.com", nameserverQueried );
  //		System.out.println( "\nNameServer: " + nameserver );
  //
  //		nameserverQueried.add(1);
  //		nameserver = LocalNameServer.getClosestActiveNSFromCache( "h.com", nameserverQueried );
  //		System.out.println( "\nNameServer: " + nameserver );
  //
  //		//		System.out.println("Address valid --> " + LocalNameServer.isValidAddressInCache( "h.com" ));
  //		//		while(LocalNameServer.isValidAddressInCache( "h.com" ) ) {
  //		//			//wait
  //		//		}
  //		//		System.out.println("Address valid --> " + LocalNameServer.isValidAddressInCache( "h.com" ));
  //		//		
  //		//		System.out.println("Nameserver valid --> " + LocalNameServer.isValidNameserverInCache( "h.com" ));
  //		//		while(LocalNameServer.isValidNameserverInCache( "h.com" ) ) {
  //		//			//wait
  //		//		}
  //		//		System.out.println("Nameserver valid --> " + LocalNameServer.isValidNameserverInCache( "h.com" ));
  //
  //		packet = new DNSPacket( header, "h0.com");
  //		LocalNameServer.addCacheEntry( packet );
  //		packet = new DNSPacket( header, "h1.com");
  //		LocalNameServer.addCacheEntry( packet );
  //		packet = new DNSPacket( header, "h2.com");
  //		LocalNameServer.addCacheEntry( packet );
  //		packet = new DNSPacket( header, "h3.com");
  //		LocalNameServer.addCacheEntry( packet );
  //		packet = new DNSPacket( header, "h4.com");
  //		LocalNameServer.addCacheEntry( packet );
  //		packet = new DNSPacket( header, "h5.com" );
  //		LocalNameServer.addCacheEntry( packet );
  //
  //		System.out.println(cache.size()  + " " + null + cache.stats().toString());
  //    Set<String> names = readWorkloadFile("/Users/hardeep/PlanetLabScript/Workload/lns_workload_15kNames_0.01/workload_uoepl1.essex.ac.uk");
  //    ImmutableSet<String> s = ImmutableSet.copyOf(names);
  //    System.out.println(s.size());
  //    System.out.println(s.toString());
  //    System.out.println(names.toString());
  //    System.out.println(s.contains("804"));
  //}
}

/**
 * When we emulate ping latencies between LNS and NS, this task will actually send packets to NS.
 * See option StartLocalNameServer.emulatePingLatencies
 */
class SendMessageWithDelay extends TimerTask {

  /**
   * Json object to send
   */
  JSONObject json;
  /**
   * Name server to send this packet to.
   */
  int nameServer;

  public SendMessageWithDelay(JSONObject json, int nameServer) {
    this.json = json;
    this.nameServer = nameServer;
  }

  @Override
  public void run() {
    LocalNameServer.sendToNSActual(json, nameServer);
  }
}