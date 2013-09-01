package edu.umass.cs.gns.localnameserver;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import edu.umass.cs.gns.httpserver.GnsHttpServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.NameAndRecordKey;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.packet.ConfirmUpdateLNSPacket;
import edu.umass.cs.gns.packet.DNSPacket;
import edu.umass.cs.gns.packet.RequestActivesPacket;
import edu.umass.cs.gns.packet.TinyQuery;
import edu.umass.cs.gns.replicationframework.BeehiveDHTRouting;
import edu.umass.cs.gns.util.BestServerSelection;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import edu.umass.cs.gns.util.UpdateTrace;

import java.io.*;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * ************************************************************
 * This class represents the functions of a Local Name Server
 *
 * @author Hardeep Uppal **********************************************************
 */
public class LocalNameServer {

//  public static Timer timer = new Timer();
  public  static ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(5);
//  public static Timer experimentSendRequestTimer = new Timer();
  /**
   * Local Name Server ID *
   */
  public static int nodeID;
  /**
   * The starting address of a block of ports to use for TCP and UDP sockets
   */
  public static int startingPort = GNS.startingPort;
  /**
   * Map of information about queries transmitted. Key: QueryId, Value: QueryInfo (id, name, time etc.)
   *
   */
  private static ConcurrentMap<Integer, QueryInfo> queryTransmittedMap;
  private static ConcurrentMap<Integer, UpdateInfo> updateTransmittedMap;
  /**
   * Cache of Name records Key: Name, Value: CacheEntry (DNS record)
   *
   */
  //private static Cache<NameAndRecordKey, CacheEntry> cache;
  private static Cache<String, CacheEntry> cache;
  /**
   * Map of name record statistic *
   */
  //private static ConcurrentMap<NameAndRecordKey, NameRecordStats> nameRecordStatsMap;
  private static ConcurrentMap<String, NameRecordStats> nameRecordStatsMap;

  /**
   * Unique and random query ID *
   */
  private static Random randomID;
  /**
   * UDP Socket for transmitting queries and listening for response *
   */
  public static DatagramSocket socket;
  /**
   * ImmutableSet containing names that can be queried by the local name server *
   */
  public static ImmutableSet<String> workloadSet;
  public static List<String> lookupTrace;
  public static List<UpdateTrace> updateTrace;
//  public static Map<String, List<OptimalNameServerInfo>> optimalReplicationInfo;

  public static BeehiveDHTRouting beehiveDHTRouting;
  public static ConcurrentHashMap<Integer, Double> nameServerLoads;
  public static long startTime;
  
  /**
   * ************************************************************
   * Constructs a local name server and assigns it a node id.
   *
   * @param nodeID Local Name Server Id
   * @throws IOException ***********************************************************
   */
  public LocalNameServer(int nodeID) throws IOException {
    LocalNameServer.nodeID = nodeID;

    queryTransmittedMap = new ConcurrentHashMap<Integer, QueryInfo>(10, 0.75f, 3);
    updateTransmittedMap = new ConcurrentHashMap<Integer, UpdateInfo>(10, 0.75f, 3);

    randomID = new Random(System.currentTimeMillis());

    cache = CacheBuilder.newBuilder().concurrencyLevel(5).maximumSize(StartLocalNameServer.cacheSize).build();
    //nameRecordStatsMap = new ConcurrentHashMap<NameAndRecordKey, NameRecordStats>(16, 0.75f, 5);
    nameRecordStatsMap = new ConcurrentHashMap<String, NameRecordStats>(16, 0.75f, 5);
    //read the workload from a file and create a immutable Set
    
    if (StartLocalNameServer.workloadFile != null) {
      workloadSet = ImmutableSet.copyOf(readWorkloadFile(StartLocalNameServer.workloadFile));
      if (StartLocalNameServer.debugMode) GNS.getLogger().info("Workload: " + workloadSet.toString());
    }

    if (StartLocalNameServer.lookupTraceFile != null) {
      lookupTrace = readTrace(StartLocalNameServer.lookupTraceFile);
    }

    if (StartLocalNameServer.updateTraceFile != null) {
      updateTrace = readUpdateTrace(StartLocalNameServer.updateTraceFile);
    }

//    if (StartLocalNameServer.optimalReplication) {
//      optimalReplicationInfo = readOptimalTrace(StartLocalNameServer.optimalTrace);
//    }

    // name server loads initialized.
    if (StartLocalNameServer.loadDependentRedirection) {
      initializeNameServerLoadMonitoring();
    }

    // redirection according to beehive replication
//    if (StartLocalNameServer.beehiveReplication) {
//      beehiveDHTRouting = new BeehiveDHTRouting();
//    }

  }

  public void initializeNameServerLoadMonitoring() {
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

//  public static Map<String, List<OptimalNameServerInfo>> readOptimalTrace(String filename) throws IOException {
//    BufferedReader br = new BufferedReader(new FileReader(filename));
//    Map<String, List<OptimalNameServerInfo>> map = new HashMap<String, List<OptimalNameServerInfo>>();
//    while (br.ready()) {
//      final String line = br.readLine().trim();
//      if (line != null) {
//        List<OptimalNameServerInfo> infoList = parseOptimalInfo(line);
//        if (infoList != null && infoList.size() > 0) {
//          String name = infoList.get(0).name;
//          map.put(name, infoList);
//        }
//      }
//    }
//    return map;
//  }

//  private static List<OptimalNameServerInfo> parseOptimalInfo(String line) {
//    if (line == null || line.trim().equals("")) {
//      return null;
//    }
//
//    String[] nameInfo = line.split("\t");
//    if (nameInfo.length == 0) {
//      return null;
//    }
//
//    List<OptimalNameServerInfo> list = new ArrayList<OptimalNameServerInfo>();
//    //First element is always the name
//    String name = nameInfo[0];
//    //number of replication
//    numReplication = nameInfo.length - 1;
//    //Other elements are replication info
//    for (int i = 1; i < nameInfo.length; i++) {
//      OptimalNameServerInfo optimalInfo = new OptimalNameServerInfo(name);
//      String[] replicationInfo = nameInfo[i].trim().split("\\|");
//      //each element is a pair of id and fraction of lookups
//      for (String idAndFrac : replicationInfo) {
//        String[] idFracPair = idAndFrac.trim().split(" ");
//        Integer id = new Integer(idFracPair[0]);
//        double fractionLookups = Double.parseDouble(idFracPair[1]);
//        optimalInfo.add(id, fractionLookups);
//      }
//      list.add(optimalInfo);
//    }
//    return list;
//  }

  /**
   * ************************************************************
   * Reads a file containing the workload for this local name server. The method returns a Set containing names that can
   * be queried by this local name server.
   *
   * @param filename Workload file
   * @return Set containing names that can be queried by this local name server.
   * @throws IOException ***********************************************************
   */
  private static Set<String> readWorkloadFile(String filename) throws IOException {
    Set<String> workloadSet = new HashSet<String>();

    BufferedReader br = new BufferedReader(new FileReader(filename));
    while (br.ready()) {
      final String name = br.readLine().trim();
      if (name != null) {
        workloadSet.add(name);
      }
    }
    return workloadSet;
  }

  private static List<String> readTrace(String filename) throws IOException {
      File file = new File(filename);
      if (!file.exists()) return  null;
    List<String> trace = new ArrayList<String>();
    BufferedReader br = new BufferedReader(new FileReader(filename));
    while (br.ready()) {
      final String name = br.readLine().trim();
      if (name != null && !name.equals("") && !name.equals("\n")) {
        trace.add(name);
      }
    }
    return trace;
  }

  private static List<UpdateTrace> readUpdateTrace(String filename) throws IOException {
    List<UpdateTrace> trace = new ArrayList<UpdateTrace>();
    BufferedReader br = new BufferedReader(new FileReader(filename));
    HashMap<String, Integer> updateCounts = new HashMap<String, Integer>();
    while (br.ready()) {
      String line = br.readLine(); //.trim();
      if (line == null) {
        continue;
      }
      line = line.trim();
      // name type (add/remove/update)
      String[] tokens = line.split("\\s+");
      if (tokens.length == 2) {
        trace.add(new UpdateTrace(tokens[0], new Integer(tokens[1])));
        continue;
      } else {
//        int count = 0;
//        if (updateCounts.containsKey(tokens[0])) {
//          count = updateCounts.get(tokens[0]) + 1;
//        }
//        updateCounts.put(tokens[0], count);
        trace.add(new UpdateTrace(tokens[0], UpdateTrace.UPDATE));
      }

    }
    br.close();
    return trace;
  }

  /**
   * ************************************************************
   * Checks whether <i>name</i> is in the workload set for this local name server.
   *
   * @param name Host/device/domain name
   * @return <i>true</i> if the workload contains <i>name</i>, <i>false</i> otherwise
   * ***********************************************************
   */
  public static boolean workloadContainsName(String name) {
    return workloadSet.contains(name);
  }

  /**
   * ************************************************************
   * Starts the Local Name Server.
   *
   * @throws Exception ***********************************************************
   */
  public void run() throws Exception {
    startTime = System.currentTimeMillis();
    System.out.println("Log level: " + GNS.getLogger().getLevel().getName());
    
    new LNSListener().start();
    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("LNS listener started.");
    
//    new LNSListenerQuery().start();
//    if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("LNS listener query started.");
//    
//    new LNSListenerUpdate().start();
//    if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("LNS listener update started.");
    
    new LNSListenerAdmin().start();
    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("LNS listener admin started.");
    
    // Abhigyan: LNSListenerResponse is merged with LNSListenerQuery.
    //		new LNSListenerResponse().start(); // Abhigyan
    //		if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("LNS listener reponse started.");
    
    // Abhigyan: address update confirmation code included in  LNSListenerUpdate
    //		new ListenerAddressUpdateConfirmation().start();
    //		if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("LNS address update confirmation started.");
    
    //Periodically send nameserver votes for location based replication
    if (StartLocalNameServer.locationBasedReplication) {
      new NameServerVoteThread(StartLocalNameServer.voteInterval).start();
    }
//          if (StartLocalNameServer.experimentMode) {
//            runSimpleTest();
//          }

    if (StartLocalNameServer.experimentMode) {
//        experimentSendRequestTimer = new Timer();
      //			if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Testing in experiment mode.");
//    	if (StartLocalNameServer.tinyQuery == false) {
//    		if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("");
//	      myInter = Intercessor.getInstance();
//	      myInter.setLocalServerID(LocalNameServer.nodeID);
////	      myInter.createTransportObject();
//    	}
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Scheduling all queries via intercessor.");
      SendQueriesViaIntercessor.schdeduleAllQueries();

      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Scheduling all updates via intercessor.");
      SendUpdatesViaIntercessor.schdeduleAllUpdates();
    }

    if (StartLocalNameServer.runHttpServer) {
      GnsHttpServer.runHttp(LocalNameServer.nodeID);
    }

    //Start sending lookups and updates

    //		if (StartLocalNameServer.isSyntheticWorkload) {
    //			if(StartLocalNameServer.optimalReplication)
    //				//new SendOptimalTraceThread().start();
    //				new SendOptimalTraceThread_Xiaozheng().start();
    //			else
    //				//new SendZipfQueryThread().start();
    //				new SendZipfQueryThread_Xiaozheng().start();
    //			//			new SendUpdatesThread().start();
    //			//			new SendUpdateMobileName( StartLocalNameServer.mobileWorkloadSize, 
    //			//					StartLocalNameServer.updateRateMobile, StartLocalNameServer.regularWorkloadSize ).start();
    //			new SendUpdateRegularName(StartLocalNameServer.regularWorkloadSize,
    //					StartLocalNameServer.alpha, StartLocalNameServer.updateRateRegular).start();
    //		} else {
    //			new SendQueryThread(NameRecordKey.EdgeRecord, StartLocalNameServer.name).start();
    //		}
  }

  // NEEDS TO BE UPDATED

//    public static void runSimpleTest() {
//        Intercessor myInter;
//        myInter = Intercessor.getInstance();
//        myInter.setLocalServerID(LocalNameServer.nodeID);
//        String name = "Abhigyan";
////        String name1 = "Khushboo";
//        String key = "Sharma";
//        String key1 = "FNU";
//        String value = "12345";
//        String value1 = "678910";
//
//        if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" Send ADD Message: " + name);
//        boolean result = myInter.sendAddRecordWithConfirmation(name, key, value);
//        if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" ADD Message Result: " + result);
//
//        if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" Send ADD Message: " + key1);
//        result = myInter.sendAddRecordWithConfirmation(name, key1, value1);
//        if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" ADD Message Result: " + result);
//
////        if (StartLocalNameServer.debugMode) GNRS.getLogger().fine(" Send LOOKUP Message");
////        QueryResultValue lookupResult = myInter.sendQuery(name,key);
////
////        if (StartLocalNameServer.debugMode) GNRS.getLogger().fine(" LOOKUP Message Result: " + lookupResult);
////        if (lookupResult.get(0).equals(value)) {
////            if (StartLocalNameServer.debugMode) GNRS.getLogger().fine(" LOOKUP SUCCESS (correct value): " + lookupResult);
////        }
////        else {
////            if (StartLocalNameServer.debugMode) GNRS.getLogger().fine(" LOOKUP FAILED (incorrect value): " + lookupResult);
////        }
//
//        for (int i = 0 ; i < 2; i++) {
//
//            if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" Send LOOKUP Message");
//            QueryResultValue lookupResult = myInter.sendQuery(name,key);
//
//            if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" LOOKUP Message Result: " + lookupResult);
//            if (lookupResult!= null && lookupResult.size() > 0 && lookupResult.get(0).equals(value)) {
//                if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" LOOKUP SUCCESS (correct value): " + lookupResult);
//            }
//            else {
//                if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" LOOKUP FAILED (incorrect value): " + lookupResult);
//            }
//
//            String newValue = SendUpdatesViaIntercessor.getRandomString();
//            if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" Send UPDATE Message new value: " + newValue);
//            boolean updateResult = myInter.sendUpdateRecordWithConfirmation(name, key, newValue, value,
//                    UpdateOperation.REPLACE_ALL);
//            if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" UPDATE Message Result: " + updateResult);
//            value = newValue;
//
//        }
//
//        if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" Send REMOVE Message");
//        boolean removeResult = myInter.sendRemoveRecordWithConfirmation(name);
//        if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" REMOVE Message Result: " + removeResult);
//
//        if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" After REMOVE: Send LOOKUP Message");
//        QueryResultValue lookupResult = myInter.sendQuery(name,key);
//        if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" After REMOVE: LOOKUP Message Result: " + lookupResult);
//        if (lookupResult!= null && lookupResult.size() > 0 && lookupResult.get(0).equals(value)) {
//            if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" After REMOVE: LOOKUP SUCCESS (correct value): " + lookupResult);
//        }
//        else {
//            if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" After REMOVE: LOOKUP FAILED (incorrect value): " + lookupResult);
//        }
//
//        for (int i = 0 ; i < 2; i++) {
//
//            if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" Send LOOKUP Message for " + key1);
//            lookupResult = myInter.sendQuery(name,key1);
//
//            if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" LOOKUP Message Result: " + lookupResult +  " for " + key1);
//            if (lookupResult!= null && lookupResult.size() > 0 && lookupResult.get(0).equals(value1)) {
//                if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" LOOKUP SUCCESS (correct value): " + lookupResult +  " for " + key1);
//            }
//            else {
//                if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" LOOKUP FAILED (incorrect value): " + lookupResult +  " for " + key1);
//            }
//
//            String newValue = SendUpdatesViaIntercessor.getRandomString();
//            if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" Send UPDATE Message new value: " + newValue +
//                    " for " + key1);
//            boolean updateResult = myInter.sendUpdateRecordWithConfirmation(name, key1, newValue, value1,
//                    UpdateOperation.REPLACE_ALL);
//            if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" UPDATE Message Result: " + updateResult +
//                   " for " + key1);
//            value1 = newValue;
//
//        }
//
//
//        if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" Send REMOVE Message"  +  " for " + key1);
//        removeResult = myInter.sendRemoveRecordWithConfirmation(name);
//        if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" REMOVE Message Result: " + removeResult +  " for " + key1);
//
//        if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" After REMOVE: Send LOOKUP Message" +  " for " + key1);
//        lookupResult = myInter.sendQuery(name,key1);
//        if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" After REMOVE: LOOKUP Message Result: " + lookupResult +  " for " + key1);
//        if (lookupResult!= null && lookupResult.size() > 0 && lookupResult.get(0).equals(value)) {
//            if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" After REMOVE: LOOKUP SUCCESS (correct value): " + lookupResult +  " for " + key1);
//        }
//        else {
//            if (StartLocalNameServer.debugMode) GNS.getLogger().fine(" After REMOVE: LOOKUP FAILED (incorrect value): " + lookupResult  +  " for " + key1);
//        }
//
////        String newValue = SendUpdatesViaIntercessor.getRandomString();
////        if (StartLocalNameServer.debugMode) GNRS.getLogger().fine(" After REMOVE: Send UPDATE Message new value: " + newValue);
////        boolean updateResult = myInter.sendUpdateWithConfirmation(name, key, newValue, value,
////                UpdateAddressPacket.UpdateOperation.REPLACE_ALL);
////        if (StartLocalNameServer.debugMode) GNRS.getLogger().fine(" After REMOVE: UPDATE Message Result: " + updateResult);
////
////        value = newValue;
//
//
//
//
//    }

  /**
   * ************************************************************
   * Adds information of a transmitted query to a query transmitted map.
   *
   * @param name Host/Domain name
   * @param nameserverID Name server Id
   * @param time System time during transmission
   * @return A unique id for the query ***********************************************************
   */
  public static int addQueryInfo(String name, NameRecordKey recordKey,
          int nameserverID, long time, String queryStatus, int lookupNumber,
          DNSPacket incomingPacket, InetAddress senderAddress, int senderPort) {
    int id = randomID.nextInt();

    //Generate unique id for the query
    while (queryTransmittedMap.containsKey(id)) {
      id = randomID.nextInt();
    }

    //Add query info
    QueryInfo query = new QueryInfo(id, name, recordKey, time,
            nameserverID, queryStatus, lookupNumber,
            incomingPacket, senderAddress, senderPort);
    queryTransmittedMap.put(id, query);
    return id;
  }

  /**
   * Same as the other addQueryInfo object. QueryID is already specified instead of being randomly chosen.
   *
   * @param name
   * @param recordKey
   * @param nameserverID
   * @param time
   * @param queryStatus
   * @param lookupNumber
   * @param incomingPacket
   * @param senderAddress
   * @param senderPort
   * @param queryID
   * @return
   */
  public static int addQueryInfo(String name, NameRecordKey recordKey,
          int nameserverID, long time, String queryStatus, int lookupNumber,
          DNSPacket incomingPacket, InetAddress senderAddress, int senderPort, int queryID) {


    //Add query info
    QueryInfo query = new QueryInfo(queryID, name, recordKey, time,
            nameserverID, queryStatus, lookupNumber,
            incomingPacket, senderAddress, senderPort);
    queryTransmittedMap.put(queryID, query);
    return queryID;
  }

  public static int addQueryInfo(String name, NameRecordKey recordKey,
          int nameserverID, long time, String queryStatus, int lookupNumber) {
    // ABHIGYAN
    return 0;

    // DUMMY FUNCTION
  }

//  public static int addUpdateInfo(String name, int nameserverID, long time) {
//    int id = randomID.nextInt();
//    //Generate unique id for the query
//    while (updateTransmittedMap.containsKey(id)) {
//      id = randomID.nextInt();
//    }
//
//    //Add update info
//    UpdateInfo update = new UpdateInfo(id, name, time, nameserverID);
//    updateTransmittedMap.put(id, update);
//    return id;
//  }
  public static int addUpdateInfo(String name, int nameserverID, long time, String senderAddress, int senderPort) {
    int id = randomID.nextInt();
    //Generate unique id for the query
    while (updateTransmittedMap.containsKey(id)) {
      id = randomID.nextInt();
    }

    //Add update info
    UpdateInfo update = new UpdateInfo(id, name, time, nameserverID, senderAddress, senderPort);
    updateTransmittedMap.put(id, update);
    return id;
  }

  /**
   * ************************************************************
   * Appends <i>status</i> to query info status.
   *
   * @param id Query Id
   * @param status Query status ***********************************************************
   */
//  public static void appendQueryInfoStatus(int id, String status) {
//    QueryInfo info = queryTransmittedMap.get(id);
//    if (info != null) {
//      info.appendQueryStatus(status);
//      //			info.queryStatus = (info.queryStatus == null
//      //					|| (info.queryStatus.equals(QueryInfo.SINGLE_TRANSMISSION)
//      //							&& status.equals(QueryInfo.MULTIPLE_TRANSMISSION)))
//      //							? status : info.queryStatus + "-" + status;
//    }
//  }

//  public static boolean addNameServerQueried(int queryID, int nameServerID, String queryStatus) {
//    QueryInfo info = queryTransmittedMap.get(queryID);
//    if (info == null) {
//      return false;
//    }
//    return info.addNameServerQueried(nameServerID, queryStatus);
//  }

  /**
   * ************************************************************
   * Removes and returns QueryInfo entry from the map for a query Id..
   *
   * @param id Query Id ***********************************************************
   */
  public static QueryInfo removeQueryInfo(int id) {
    return queryTransmittedMap.remove(id);
  }

  
  public static UpdateInfo removeUpdateInfo(int id) {
    return updateTransmittedMap.remove(id);
  }



  public static UpdateInfo getUpdateInfo(int id) {
    return updateTransmittedMap.get(id);
  }
  
  /**
   * ************************************************************
   * Returns true if the map contains the specified query id, false otherwise.
   *
   * @param id Query Id ***********************************************************
   */
  public static boolean containsQueryInfo(int id) {
    return queryTransmittedMap.containsKey(id);
  }

  public static QueryInfo getQueryInfo(int id) {
    return queryTransmittedMap.get(id);
  }

  /**
   * ************************************************************
   * Signal sender that listener has the data and the sender can proceed.
   *
//   * @param queryId
   * @return ***********************************************************
   */
//  public static boolean hasDataToProceed(int queryId) {
//    QueryInfo info = queryTransmittedMap.get(queryId);
//    return info == null || info.hasDataToProceed();
//  }

//  public static boolean waitUntilHasDataToProceedOrTimeout(int queryId, long waitingTime) {
//    QueryInfo info = queryTransmittedMap.get(queryId);
//    return info.waitUntilHasDataToProceedOrTimeout(waitingTime);
//  }

//  public static void setDataToProceed(int queryId, boolean hasData) {
//    QueryInfo info = queryTransmittedMap.get(queryId);
//    if (info != null) {
//      info.setHasDataToProceed(hasData);
//    }
//  }

  public static void invalidateCache() {
    cache.invalidateAll();
  }

  /**
   * ************************************************************
   * Returns true if the local name server cache contains DNS record for the specified name, false otherwise
   *
   * @param name Host/Domain name ***********************************************************
   */
  public static boolean containsCacheEntry(String name //,NameRecordKey recordKey
          ) {
    //return cache.getIfPresent(new NameAndRecordKey(name, recordKey)) != null;
    return cache.getIfPresent(name) != null;
  }

  /**
   * ************************************************************
   * Discards any cached value for key (name)
   *
   * @param name Key ***********************************************************
   */
  public static void removeCacheEntry(String name, NameRecordKey recordKey) {
    cache.invalidate(new NameAndRecordKey(name, recordKey));
  }

  public static void incrementLookupRequest(String name//, NameRecordKey recordKey
          ) {
//    NameAndRecordKey nameAndType = new NameAndRecordKey(name, recordKey);
//    if (nameRecordStatsMap.containsKey(nameAndType)) {
//      nameRecordStatsMap.get(nameAndType).incrementLookupCount();
//    } else {
//      NameRecordStats nameRecordStats = new NameRecordStats();
//      nameRecordStats.incrementLookupCount();
//      nameRecordStatsMap.put(nameAndType, nameRecordStats);
//    }
    
    if (nameRecordStatsMap.containsKey(name)) {
      nameRecordStatsMap.get(name).incrementLookupCount();
    } else {
      NameRecordStats nameRecordStats = new NameRecordStats();
      nameRecordStats.incrementLookupCount();
      nameRecordStatsMap.put(name, nameRecordStats);
    }
  }

  public static void incrementUpdateRequest(String name//, NameRecordKey recordKey
          ) {
//    NameAndRecordKey nameAndType = new NameAndRecordKey(name, recordKey);
//    if (nameRecordStatsMap.containsKey(nameAndType)) {
//      nameRecordStatsMap.get(nameAndType).incrementUpdateCount();
//    } else {
//      NameRecordStats nameRecordStats = new NameRecordStats();
//      nameRecordStats.incrementUpdateCount();
//      nameRecordStatsMap.put(nameAndType, nameRecordStats);
//    }
    if (nameRecordStatsMap.containsKey(name)) {
      nameRecordStatsMap.get(name).incrementUpdateCount();
    } else {
      NameRecordStats nameRecordStats = new NameRecordStats();
      nameRecordStats.incrementUpdateCount();
      nameRecordStatsMap.put(name, nameRecordStats);
    }
  }

  public static void incrementLookupResponse(String name//, NameRecordKey recordKey
          ) {
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

  public static void incrementUpdateResponse(String name //, NameRecordKey recordKey
          ) {
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
   * ************************************************************
   * Returns the lookup vote for <i>name</i>.
   *
   * @param name Host/domain/device name ***********************************************************
   */
  public static int getVotes(String name//, NameRecordKey recordKey
          ) {
//    NameAndRecordKey nameAndType = new NameAndRecordKey(name, recordKey);
//    NameRecordStats nameRecordStats = nameRecordStatsMap.get(nameAndType);
//    int vote = (nameRecordStats != null) ? nameRecordStats.getVotes() : 0;
//    return vote;
   
    NameRecordStats nameRecordStats = nameRecordStatsMap.get(name);
    int vote = (nameRecordStats != null) ? nameRecordStats.getVotes() : 0;
    return vote;
  }

  public static Set<String> getNameRecordStatsKeySet() {
    return nameRecordStatsMap.keySet();
  }

  /**
   * ************************************************************
   * Adds a new CacheEntry (NameRecord) from a DNS packet. Overwrites existing cache entry for a name, if the name
   * record exist in the cache.
   *
   * @param packet DNS packet containing record ***********************************************************
   */
  public static CacheEntry addCacheEntry(DNSPacket packet) {
    CacheEntry entry = new CacheEntry(packet);
    cache.put(entry.getName(), entry);
      return entry;
  }
  
  public static void addCacheEntry(TinyQuery packet) {
    CacheEntry entry = new CacheEntry(packet);
    cache.put(entry.getName(), entry);
  }

  
  public static void addCacheEntry(RequestActivesPacket packet) {
    CacheEntry entry = new CacheEntry(packet);
    cache.put(entry.getName(), entry);
  }

  
//  public static void addCacheEntry(AddRecordPacket packet) {
//    CacheEntry entry = new CacheEntry(packet);
//    cache.put(new NameAndRecordKey(entry.name, entry.recordKey), entry);
//  }
  /**
   * ************************************************************
   * Updates an existing cache entry with new information from a DNS packet.
   *
   * @param packet DNS packet containing record ***********************************************************
   */
  public static CacheEntry updateCacheEntry(DNSPacket packet) {
    CacheEntry entry = cache.getIfPresent(packet.getQname());
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
	  


  public static void updateCacheEntry(TinyQuery tinyQuery) {
    CacheEntry entry = cache.getIfPresent(tinyQuery.getName());
    if (entry == null) {
      return;
    }
    entry.updateCacheEntry(tinyQuery);
  }
  

  public static void updateCacheEntry(ConfirmUpdateLNSPacket packet) {
    switch (packet.getType()) {
      case CONFIRM_ADD_LNS:
        // screw it.. let the next query generate the cache
        break;
      case CONFIRM_REMOVE_LNS:
        cache.invalidate(packet.getName());
        break;
      case CONFIRM_UPDATE_LNS:
        CacheEntry entry = cache.getIfPresent(packet.getName());
        if (entry != null) {
          entry.updateCacheEntry(packet);
        }
        break;
    }
  }

//  /**
//   * ************************************************************
//   * Updates an existing cache entry' active name server set with <i>activeNameServers</i> list.
//   *
//   * @param name Host/device/domain name
//   * @param activeNameServers List of active name server ***********************************************************
//   */
//  public static void updateActiveNameServerCacheEntry(String name, //NameRecordKey recordKey,
//          Set<Integer> activeNameServers) {
//    CacheEntry entry = cache.getIfPresent(name);
//    if (entry == null) {
//      return;
//    }
//    entry.updateActiveNameServerCacheEntry(activeNameServers);
//  }

  /**
   * ************************************************************
   * Returns a cache entry for the specified name. Returns null if the cache does not have the key mapped to an entry
   *
   * @param name Host/Domain name ***********************************************************
   */
  public static CacheEntry getCacheEntry(String name//, NameRecordKey recordKey
          ) {
    return cache.getIfPresent(name);
  }

  /**
   * ************************************************************
   * Returns a Set containing name and CacheEntry 
   */
  public static Set<Map.Entry<String, CacheEntry>> getCacheEntrySet() {
    return cache.asMap().entrySet();
  }



  /**
   * ************************************************************
   * Checks the validity of active nameserver set in cache.
   *
   * @param name Host/device/domain name whose name record is cached.
   * @return Returns true if the entry is valid, false otherwise
   * ***********************************************************
   */
  public static boolean isValidNameserverInCache(String name//, NameRecordKey recordKey
          ) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    return (cacheEntry != null) ? cacheEntry.isValidNameserver() : false;
  }

  public static int timeSinceAddressCached(String name, NameRecordKey recordKey
          ) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    return (cacheEntry != null) ? cacheEntry.timeSinceAddressCached(recordKey) : -1;
  }

  /**
   * Invalidates the active name server set in cache by setting its value to <i>null</i>.
   *
   * @param name
   */
  public static void invalidateActiveNameServer(String name//, NameRecordKey recordKey
          ) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    if (cacheEntry != null) {
      cacheEntry.invalidateActiveNameServer();
    }
  }

  /**
   * ************************************************************
   * Returns the closest active name server from cache that is not in <i>nameserverQueried</i>.' Returns -1 if the cache
   * does not contain <i>name</i> or if all active name servers are unavailable.
   *
   * @param name Host/Domain/Device Name
   * @param nameserverQueried A set of name servers queried and excluded.
   * ***********************************************************
   */
  public static int getClosestActiveNSFromCache(String name, //NameRecordKey recordKey,
          Set<Integer> nameserverQueried) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    if (cacheEntry == null || cacheEntry.getActiveNameServers() == null) {
      return -1;
    }
    return BestServerSelection.getSmallestLatencyNS(cacheEntry.getActiveNameServers(), nameserverQueried);
  }

  /**
   * ************************************************************
   * select best name server considering ping-latency + server-load including actives and primaries
   *
   *
   * @param name Host/Domain/Device Name
   * @param nameserverQueried A set of name servers queried and excluded.
   * @return id of best name server, -1 if none found. ***********************************************************
   */
  public static int getBestActiveNameServerFromCache(String name, //NameRecordKey recordKey, 
          Set<Integer> nameserverQueried) {
	  
    CacheEntry cacheEntry = cache.getIfPresent(name);
    if (cacheEntry == null || cacheEntry.getActiveNameServers() == null) {
      return -1;
    }
    
    HashSet<Integer> allServers = new HashSet<Integer>();
    if (cacheEntry.getActiveNameServers() != null) {
      for (int x : cacheEntry.getActiveNameServers()) {
        if (!allServers.contains(x) && nameserverQueried != null && !nameserverQueried.contains(x)) {
          allServers.add(x);
        }
      }
    }

//    for (int x : cacheEntry.getPrimaryNameServer()) {
//      if (!allServers.contains(x) && nameserverQueried != null && !nameserverQueried.contains(x)) {
//        allServers.add(x);
//      }
//    }

    return BestServerSelection.simpleLatencyLoadHeuristic(allServers);
  }
  
  static Random r = new Random();
  
  public static int getLoadAwareBeehiveNameServerFromCache(String name, //NameRecordKey recordKey,
          Set<Integer> nameserverQueried) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    if (cacheEntry == null) {
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("BEEHIVE No Cache Entry: " + -1);
      return -1;
    }

    ArrayList<Integer> allServers = new ArrayList<Integer>();
    if (cacheEntry.getActiveNameServers() != null) {
      for (int x : cacheEntry.getActiveNameServers()) {
        if (!allServers.contains(x) && nameserverQueried != null && !nameserverQueried.contains(x)) {
          allServers.add(x);
        }
      }
    }

    for (int x : cacheEntry.getPrimaryNameServer()) {
      if (!allServers.contains(x) && nameserverQueried != null && !nameserverQueried.contains(x)) {
        allServers.add(x);
      }
    }
    if (allServers.size() == 0) {
      return -1;
    }

    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("BEEHIVE All Name Servers: " + allServers);
    if (allServers.contains(ConfigFileInfo.getClosestNameServer())) {
      return ConfigFileInfo.getClosestNameServer();
    }
    return allServers.get(r.nextInt(allServers.size()));

//		int x = beehiveDHTRouting.getDestNS(new Integer(name), 
//				ConfigFileInfo.getClosestNameServer(), allServers);
//		if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("BEEHIVE Chosen Name Server: " + x);
//		return x;

  }


  private static int beehiveNSChoose(int closestNS, ArrayList<Integer> nameServers, Set<Integer> nameServersQueried) {
  	
  	if (nameServers.contains(closestNS) && (nameServersQueried == null || !nameServersQueried.contains(closestNS))) 
  		return closestNS;
  	
  	Collections.sort(nameServers);
  	for (int x:nameServers){
  		if (x > closestNS && (nameServersQueried == null || !nameServersQueried.contains(x))) {
  			return x;
  		}
  	}
  	
  	for (int x:nameServers){
  		if (x < closestNS  && (nameServersQueried == null || !nameServersQueried.contains(x))) {
  			return x;
  		}
  	}
  	
  	return -1;
  }
  
  public static int getBeehiveNameServerFromCache(String name, //NameRecordKey recordKey,
          Set<Integer> nameserverQueried) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
    if (cacheEntry == null) {
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("BEEHIVE No Cache Entry: " + -1);
      return -1;
    }

    ArrayList<Integer> allServers = new ArrayList<Integer>();
    if (cacheEntry.getActiveNameServers() != null) {
      for (int x : cacheEntry.getActiveNameServers()) {
        if (!allServers.contains(x) && nameserverQueried != null && !nameserverQueried.contains(x)) {
          allServers.add(x);
        }
      }
    }

    for (int x : cacheEntry.getPrimaryNameServer()) {
      if (!allServers.contains(x) && nameserverQueried != null && !nameserverQueried.contains(x)) {
        allServers.add(x);
      }
    }
    if (allServers.size() == 0) {
      return -1;
    }

    if (StartLocalNameServer.debugMode) GNS.getLogger().fine("BEEHIVE All Name Servers: " + allServers);
    if (allServers.contains(ConfigFileInfo.getClosestNameServer())) {
      return ConfigFileInfo.getClosestNameServer();
    }
    return beehiveNSChoose(ConfigFileInfo.getClosestNameServer(), allServers, nameserverQueried);
//    return allServers.get(r.nextInt(allServers.size()));

    //		int x = beehiveDHTRouting.getDestNS(new Integer(name), 
    //				ConfigFileInfo.getClosestNameServer(), allServers);
    //		if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("BEEHIVE Chosen Name Server: " + x);
    //		return x;

  }

  /**
   * ************************************************************
   * Returns the closest name server (active and primary) from cache that is not in <i>nameserverQueried</i>.' Returns
   * -1 if the cache does not contain <i>name</i> or if all name servers are unavailable.
   *
   * @param name Host/Domain/Device Name
   * @param nameserverQueried A set of name servers queried and excluded.
   * ***********************************************************
   */
  public static int getClosestActiveNameServerFromCache(String name, //NameRecordKey recordKey,
          Set<Integer> nameserverQueried) {
    CacheEntry cacheEntry = cache.getIfPresent(name);
//      if (StartLocalNameServer.debugMode) GNRS.getLogger().fine(" Name " + name + "ACTIVE NAME SERVERS " + cacheEntry.getActiveNameServers());
    return (cacheEntry != null && cacheEntry.getActiveNameServers() != null)
            ? BestServerSelection.getSmallestLatencyNS(cacheEntry.getActiveNameServers(), nameserverQueried) : -1;
  }

  /**
   * ************************************************************
   * Return a Set containing ids of primary replica for <i>name</i>
   *
   * @param name
   * @return
   * @throws UnsupportedEncodingException
   * @throws NoSuchAlgorithmException ***********************************************************
   */
  public static Set<Integer> getPrimaryNameServers(String name//, NameRecordKey recordKey
          ) {
    //NameAndRecordKey nameAndType = new NameAndRecordKey(name, recordKey);
    //CacheEntry cacheEntry = cache.getIfPresent(nameAndType);
    CacheEntry cacheEntry = cache.getIfPresent(name);
    //if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("GNRS Logger: Current cache entry: " + cacheEntry + " Name = " + name + " record " + recordKey);
    return (cacheEntry != null) ? cacheEntry.getPrimaryNameServer() : HashFunction.getPrimaryReplicas(name);
  }

  /**
   * ************************************************************
   * Returns the closest primary name server for <i>name</i>.
   *
   * @return Closest primary name server for <i>name</i>, or -1 if no such name server is present.
   * ***********************************************************
   */
  public static int getClosestPrimaryNameServer(String name, Set<Integer> nameServersQueried) {
    try {
      Set<Integer> primary = getPrimaryNameServers(name);
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Primary Name Servers: " + primary.toString() + " for name: " + name);
      int x = BestServerSelection.getSmallestLatencyNS(primary, nameServersQueried);
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Closest Primary Name Server: " + x + " NS Queried: " + nameServersQueried);
      return x;
    } catch (Exception e) {
      return -1;
    }
  }

  /**
   * ************************************************************
   * Returns the best primary name server (latency + serverload) for <i>name</i>.
   *
   * @return best primary name server for <i>name</i>, or -1 if no such name server is present.
   * ***********************************************************
   */
  public static int getBestPrimaryNameServer(String name, //NameRecordKey recordKey,
          Set<Integer> nameserverQueried) {
    try {
      Set<Integer> primary = getPrimaryNameServers(name);

      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Primary Name Servers: " + primary.toString());
      HashSet<Integer> allServers = new HashSet<Integer>();
      for (int x : primary) {
        if (!allServers.contains(x) && nameserverQueried != null
                && !nameserverQueried.contains(x)) {
          allServers.add(x);
        }
      }
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("All Servers: " + allServers.toString());
      int x = BestServerSelection.simpleLatencyLoadHeuristic(allServers);
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Best Primary Name Server: " + x);
      return x;
    } catch (Exception e) {
      return -1;
    }
  }
//  static Random r = new Random();

  /**
   * ************************************************************
   * Returns the primary name server that would be selected by beehive among primaries for <i>name</i>.
   *
   * @return beehive chosen primary name server for <i>name</i>, or -1 if no such name server is present.
   * ***********************************************************
   */
  public static int getBeehivePrimaryNameServer(String name, //NameRecordKey recordKey,
          Set<Integer> nameserverQueried) {
  	
    try {
      Set<Integer> primary = getPrimaryNameServers(name);
      
      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Primary Name Servers: " + primary.toString());
      ArrayList<Integer> allServers = new ArrayList<Integer>();
      
      for (int x : primary) {
        if (!allServers.contains(x) && nameserverQueried != null
                && !nameserverQueried.contains(x)) {
          allServers.add(x);
        }
      }

      if (allServers.size() == 0) {
        return -1;
      }

      if (allServers.contains(ConfigFileInfo.getClosestNameServer())) {
        return ConfigFileInfo.getClosestNameServer();
      }
      
      return beehiveNSChoose(ConfigFileInfo.getClosestNameServer(), allServers, nameserverQueried);
//      return allServers.get(r.nextInt(allServers.size()));

      //			if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("All Servers: " + allServers.toString());
      //			int a = new Integer(name);
      //			if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Name int: " + a);
      //			int b = ConfigFileInfo.getClosestNameServer();
      //			if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("Closest NS: " + b);
      //			
      //			int x = beehiveDHTRouting.getDestNS(a, b, allServers);
      //			if (StartLocalNameServer.debugMode) GNRS.getLogger().fine("BEEHIVE Primary Name Server: " + x);
      //			
      //			return x;
    } catch (Exception e) {
      StringBuilder s = new StringBuilder();
      for (StackTraceElement s1 : e.getStackTrace()) {
        s.append(s1.toString() + "\n");
      }

      if (StartLocalNameServer.debugMode) GNS.getLogger().fine("Beehive Primary ERROR: " + s.toString());
      return -1;
    }
  }

  /**
   * ************************************************************
   * Generates and stores lookup query statistic with the given parameters.
   *
   * @param name Host/domain/device name
   * @param latency Latency for lookup query
   * @param numTransmission Number of transmissions
   * @param activeNameServerId Active name server id
   * @param timestamp Response timestamp
   * @return A tab seperated lookup query stats with the following format <i>Name Latency(ms) PingLatency(ms)
   * NumTransmissions ActiveNameServerId LocalNameServerId ResponseTimeStamp</i>
   * ***********************************************************
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
   * ************************************************************
   * Prints local name server cache ***********************************************************
   */
  public static String cacheLogString(String preamble) {
    StringBuilder cacheTable = new StringBuilder();

    for (CacheEntry entry : cache.asMap().values()) {
      cacheTable.append("\n");
      cacheTable.append(entry.toString());
    }
    return preamble + cacheTable.toString();
  }

  /**
   * ************************************************************
   * Prints information about transmitted queries that have not received a response.
   * ***********************************************************
   */
  public static String queryLogString(String preamble) {
    StringBuilder queryTable = new StringBuilder();
    for (QueryInfo info : LocalNameServer.queryTransmittedMap.values()) {
      queryTable.append("\n");
      queryTable.append(info.toString());
    }
    return preamble + queryTable.toString();
  }

  /**
   * ************************************************************
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

  /**
   * Test *
   */
  public static void main(String[] args) throws IOException {
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
  }
}
