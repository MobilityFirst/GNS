package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.ReplicationFrameworkType;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.recordmap.BasicRecordMap;
import edu.umass.cs.gns.nameserver.replicacontroller.ComputeNewActivesTask;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gns.nio.ByteStreamToJSONObjects;
import edu.umass.cs.gns.nio.NioServer;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.replicationframework.ReplicationFrameworkInterface;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import edu.umass.cs.gns.util.MovingAverage;
import edu.umass.cs.gns.util.Util;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class NameServer {

  /**
   * Nameserver's id *
   */
  public static int nodeID;
  /**
   * UDP socket over which DNSPackets are received and sent *
   */
//  public static DatagramSocket dnsSocket;
  public static BasicRecordMap recordMap;
  public static BasicRecordMap replicaController;
  public static ReplicationFrameworkInterface replicationFramework;
  public static MovingAverage loadMonitor;
  public static NioServer tcpTransport;
  public static NSPacketDemultiplexer nsDemultiplexer;
  public static Timer timer = new Timer();
  public static ScheduledThreadPoolExecutor executorService;
  public static PaxosManager paxosManager;

  /**
   * Only used during experiments.
   */
  public static int initialExpDelayMillis = 30;

  /**
   * Constructs a name server which uses a synthetic workload of integers as names in its record table. The size of the
   * workload is used to generate records and the integer value represents the name and its popularity.
   *
   * @param nodeID Name server id
   * @throws IOException
   */
  public NameServer(int nodeID) throws IOException {

    NameServer.nodeID = nodeID;
    GNS.getLogger().info("GNS Version: " + GNS.readBuildVersion() + "\n");

    GNS.getLogger().info("NS Node " + NameServer.nodeID + " using " + StartNameServer.dataStore.toString() + " data" +
            " store");


    // THIS IS WHERE THE NAMESERVER DELEGATES TO THE APPROPRIATE BACKING STORE
    NameServer.recordMap = (BasicRecordMap) Util.createObject(StartNameServer.dataStore.getClassName(),
            // probably should use something more generic here
            MongoRecords.DBNAMERECORD);
    // Ditto for the replica controller records
    NameServer.replicaController = (BasicRecordMap) Util.createObject(StartNameServer.dataStore.getClassName(),
            // probably should use something more generic here
            MongoRecords.DBREPLICACONTROLLER);

    resetDB();

    NameServer.replicationFramework = ReplicationFrameworkType.instantiateReplicationFramework(
            StartNameServer.replicationFramework);

    // Executor service created.
    executorService = new ScheduledThreadPoolExecutor(StartNameServer.workerThreadCount);

    // Non-blocking IO created
    nsDemultiplexer = new NSPacketDemultiplexer();

    // start listening on UDP socket
    new NSListenerUDP().start();

    // create a TCP transport object because we need to pass it to paxos manager.
    // Don't start the listening socket because paxos manager is not initialized yet. Also,
    // we are still doing log recovery and don't want to process new messages.
    ByteStreamToJSONObjects worker = new ByteStreamToJSONObjects(nsDemultiplexer);
    tcpTransport = new NioServer(nodeID, worker, new GNSNodeConfig());

    // create paxos manager and do log recovery (if logs exist). paxos manager wont send any messages yet.
    paxosManager = new PaxosManager(ConfigFileInfo.getNumberOfNameServers(), nodeID, tcpTransport,
            new NSPaxosInterface(), executorService, StartNameServer.paxosLogFolder);

    // start listening socket
    new Thread(tcpTransport).start();

    // now listening socket is running, start failure detector and other process in paxos which require sending messages.
    paxosManager.startPaxos(StartNameServer.failureDetectionPingInterval, StartNameServer.failureDetectionTimeoutInterval);


    // START ADMIN THREAD - DO NOT REMOVE THIS
    if(StartLocalNameServer.experimentMode == false) new NSListenerAdmin().start(); // westy

    if (StartNameServer.experimentMode) {
      try {
        Thread.sleep(initialExpDelayMillis); // Abhigyan: wait so that other name servers can bind to respective TCP ports.
      } catch (InterruptedException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }

    // Load monitoring calculation initalized.
    loadMonitor = new MovingAverage(StartNameServer.loadMonitorWindow);

    timer.schedule(new WriteMemUsage(), 100000, 100000); // write stats about system

  }

  public void run() {

    // start paxos manager first.

    // this will recover state from paxos logs, if it exists


    createPrimaryPaxosInstances();

    if (StartNameServer.experimentMode) {

      if (StartNameServer.nameActives == null) {
        GenerateSyntheticRecordTable.generateRecordTableBulkInsert(StartNameServer.regularWorkloadSize,
                StartNameServer.mobileWorkloadSize, StartNameServer.TTLRegularName,
                StartNameServer.TTLMobileName);
      } else {
        GenerateSyntheticRecordTable.generateRecordTableWithActivesNew(StartNameServer.nameActives);
      }

    }

    // schedule periodic computation of new active name servers.
    if (!(StartNameServer.replicationFramework == ReplicationFrameworkType.STATIC ||
            StartNameServer.replicationFramework == ReplicationFrameworkType.OPTIMAL)) {

      // Abhigyan: commented this because we are using lns votes instead of stats send by actives to decide replication
      // TODO  longer term solution is to integrate IP geo-location database at name servers.
//        executorService.scheduleAtFixedRate(new SendNameRecordStats(),
//                (new Random()).nextInt((int) StartNameServer.aggregateInterval),
//                StartNameServer.aggregateInterval, TimeUnit.MILLISECONDS);

      long initialDelayMillis = initialExpDelayMillis + StartNameServer.analysisInterval + // wait for one interval for estimating demand
              (new Random()).nextInt((int) StartNameServer.analysisInterval); // randomize to avoid synchronization among replicas.
      if (StartNameServer.experimentMode && StartNameServer.quitAfterTimeSec > 0) {
        initialDelayMillis = initialExpDelayMillis + (long)(StartNameServer.analysisInterval*1.5);
      }
      GNS.getLogger().severe("ComputeNewActives Initial delay " + initialDelayMillis);

//      initialDelayMillis = 100000;

      executorService.scheduleAtFixedRate(new ComputeNewActivesTask(), initialDelayMillis,
              StartNameServer.analysisInterval, TimeUnit.MILLISECONDS);

      //  TODO commenting keep alive messages
//        executorService.scheduleAtFixedRate(new SenderKeepAliveRC(),
//                SenderKeepAliveRC.KEEP_ALIVE_INTERVAL_SEC + (new Random()).nextInt(SenderKeepAliveRC.KEEP_ALIVE_INTERVAL_SEC),
//                SenderKeepAliveRC.KEEP_ALIVE_INTERVAL_SEC, TimeUnit.SECONDS);
    }
  }


  public static void createPrimaryPaxosInstances() {

    ArrayList<Integer> nodesSorted = new ArrayList<Integer>();
    for (String s1: HashFunction.nsTreeMap.keySet()) {
      nodesSorted.add(HashFunction.nsTreeMap.get(s1));
    }

    for (int i = 0; i < HashFunction.nsTreeMap.size(); i++) {
      int paxosMemberIndex = i;
      String paxosID = HashFunction.getMD5Hash(Integer.toString(nodesSorted.get(paxosMemberIndex))) + "-P";
      HashSet<Integer> nodes = new HashSet<Integer>();
      boolean containsNode = false;
      while(nodes.size() < GNS.numPrimaryReplicas) {
        if (nodesSorted.get(paxosMemberIndex) == nodeID) containsNode = true;
        nodes.add(nodesSorted.get(paxosMemberIndex));
        paxosMemberIndex += 1;
        if (paxosMemberIndex == HashFunction.nsTreeMap.size()) paxosMemberIndex = 0;
      }

      if (containsNode) {
        GNS.getLogger().info("Creating paxos instances: " + paxosID + "\t" + nodes);
        NameServer.paxosManager.createPaxosInstance(paxosID, nodes, "");
      }
    }

//    for (int i = 0; i < GNS.numPrimaryReplicas; i++) {
//      Map.Entry entry = HashFunction.nsTreeMap.lowerEntry(s);
//      String paxosID = (String) entry.getKey();
//
//      s = entry.getKey();
//    }

  }

  /******************************
   * Name Record methods
   ******************************/
  /**
   * Load a name record from the backing database and retrieve all the fields.
   * @param name
   * @return
   * @throws RecordNotFoundException
   */
  public static NameRecord getNameRecord(String name) throws RecordNotFoundException {
    return recordMap.getNameRecord(name);
  }

  /**
   * Load a name record from the backing database and retrieve certain fields as well.
   * 
   * @param name
   * @param fields - a list of Field structures representing "system" fields to retrieve
   * @return
   * @throws RecordNotFoundException
   */
  public static NameRecord getNameRecordMultiField(String name, ArrayList<ColumnField> fields)
          throws RecordNotFoundException {
    return new NameRecord(recordMap.lookup(name, NameRecord.NAME, fields, NameRecord.VALUES_MAP, null));
  }

  /**
   * Load a name record from the backing database and retrieve certain fields as well.
   * 
   * @param name
   * @param fields - a list of Field structures representing "system" fields to retrieve
   * @param userFields - a list of Field structures representing user fields to retrieve
   * @return
   * @throws RecordNotFoundException
   */
  public static NameRecord getNameRecordMultiField(String name, ArrayList<ColumnField> fields, ArrayList<ColumnField> userFields)
          throws RecordNotFoundException {
    return new NameRecord(recordMap.lookup(name, NameRecord.NAME, fields, NameRecord.VALUES_MAP, userFields));
  }

  /**
   * Load a name record from the backing database and retrieve certain fields as well.
   * 
   * @param name
   * @param fields
   * @param userFieldNames - strings which name the user fields to return
   * @return
   * @throws RecordNotFoundException 
   */
  public static NameRecord getNameRecordMultiField(String name, ArrayList<ColumnField> fields, String... userFieldNames)
          throws RecordNotFoundException {
    return new NameRecord(recordMap.lookup(name, NameRecord.NAME, fields, NameRecord.VALUES_MAP, userFieldList(userFieldNames)));
  }

  private static ArrayList<ColumnField> userFieldList(String... fieldNames) {
    ArrayList<ColumnField> result = new ArrayList<ColumnField>();
    for (String name : fieldNames) {
      result.add(new ColumnField(name, ColumnFieldType.LIST_STRING));
    }
    return result;
  }

  /**
   * Add this name record to DB
   * @param record
   * @throws RecordExistsException
   */
  public static void addNameRecord(NameRecord record) throws RecordExistsException {
    recordMap.addNameRecord(record);
  }

  /**
   * Replace the name record in DB with this copy of name record
   * @param record
   */
  public static void updateNameRecord(NameRecord record) {
    recordMap.updateNameRecord(record);
  }

  /**
   * Remove name record from DB
   * @param name
   */
  public static void removeNameRecord(String name) {
    recordMap.removeNameRecord(name);
  }

  /**
   * Returns an iterator for all the rows in the collection with all fields filled in.
   * 
   * @return 
   */
  public static BasicRecordCursor getAllRowsIterator() {
    return recordMap.getAllRowsIterator();
  }

  /**
   * Given a key and a value return all the records as a BasicRecordCursor that have a *user* key with that value.
   * @param key
   * @param value
   * @return 
   */
  public static BasicRecordCursor selectRecords(String key, Object value) {
    return recordMap.selectRecords(NameRecord.VALUES_MAP, key, value);
  }

  /**
   * If key is a GeoSpatial field return all fields that are within value which is a bounding box specified as a nested JSONArray
   * string tuple of paired tuples: [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]] The returned value is a BasicRecordCursor.
   * 
   * @param key
   * @param value - a string that looks like this: [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]]
   * @return 
   */
  public static BasicRecordCursor selectRecordsWithin(String key, String value) {
    return recordMap.selectRecordsWithin(NameRecord.VALUES_MAP, key, value);
  }

  /**
   * If key is a GeoSpatial field return all fields that are near value which is a point specified as a JSONArray string tuple: 
   * [LONG, LAT]. maxDistance is in meters. The returned value is a BasicRecordCursor.
   * 
   * @param key
   * @param value - a string that looks like this: [LONG, LAT]
   * @param maxDistance - the distance in meters
   * @return 
   */
  public static BasicRecordCursor selectRecordsNear(String key, String value, Double maxDistance) {
    return recordMap.selectRecordsNear(NameRecord.VALUES_MAP, key, value, maxDistance);
  }
  
  /**
   * Returns all fields that match the query.
   * 
   * @param query
   * @return 
   */
  public static BasicRecordCursor selectRecordsQuery(String query) {
    return recordMap.selectRecordsQuery(NameRecord.VALUES_MAP, query);
  }

  /******************************
   * Replica controller methods
   ******************************/
  /**
   * Read the complete ReplicaControllerRecord from database
   * @param name
   * @return
   */
  public static ReplicaControllerRecord getNameRecordPrimary(String name) throws RecordNotFoundException {
    return replicaController.getNameRecordPrimary(name);
  }

  public static ReplicaControllerRecord getNameRecordPrimaryMultiField(String name, ColumnField... fields)
          throws RecordNotFoundException {
    return getNameRecordPrimaryMultiField(name, new ArrayList<ColumnField>(Arrays.asList(fields)));
  }

  /**
   * Read name record with select fields
   * @param name
   * @param fields
   * @return
   * @throws RecordNotFoundException
   */
  public static ReplicaControllerRecord getNameRecordPrimaryMultiField(String name, ArrayList<ColumnField> fields)
          throws RecordNotFoundException {
    return new ReplicaControllerRecord(replicaController.lookup(name, ReplicaControllerRecord.NAME, fields));
  }

  /**
   * Add this record to database
   * @param record
   */
  public static void addNameRecordPrimary(ReplicaControllerRecord record) throws RecordExistsException {
    replicaController.addNameRecordPrimary(record);
  }

  /**
   * Remove a ReplicaControllerRecord with given name from database
   * @param name
   */
  public static void removeNameRecordPrimary(String name) {
    replicaController.removeNameRecord(name);
  }

  /**
   * Replace the ReplicaControllerRecord in DB with this copy of ReplicaControllerRecord
   * @param record
   */
  public static void updateNameRecordPrimary(ReplicaControllerRecord record) {
    replicaController.updateNameRecordPrimary(record);
  }

  public static BasicRecordCursor getAllPrimaryRowsIterator() {
    return replicaController.getAllRowsIterator();
  }

  //  the nuclear option
  public static void resetDB() {
    recordMap.reset();
    // reset them both
    replicaController.reset();
  }


  /**
   * Wrapper method to send to LNS, uses either UDP/TCP depending on size of packet. uses udp for packet < 1000 bytes,
   * tcp otherwise.
   * @param json  json object to send
   * @param lns   local name server ID
   */
  public static void sendToLNS(JSONObject json, int lns) {
//    if (json.toString().length() < 1000) {
//      try {
//        NSListenerUDP.udpTransport.sendPacket(json, lns, GNS.PortType.LNS_UDP_PORT);
//      } catch (JSONException e) {
//        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//      }
//    } else { // for large packets,  use TCP
      try {
        NameServer.tcpTransport.sendToIDActual(lns, json);
      } catch (IOException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
//    }
  }
}


class WriteMemUsage extends TimerTask {
  int count = 0;
  @Override
  public void run() {
    count ++;
    GenerateSyntheticRecordTable.outputMemoryUse(Integer.toString(count) + "sec ");
    GNS.getLogger().severe("\tTasksSubmitted\t" + NameServer.executorService.getTaskCount() + "\tTasksCompleted\t" +
            NameServer.executorService.getCompletedTaskCount());
//    Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
//    for (Thread t: threadSet) {
//      StackTraceElement[] traceElements = t.getStackTrace();
//      StringBuilder sb = new StringBuilder();
//      for (StackTraceElement e: traceElements) {
//        sb.append(e.toString());
//        sb.append("\n");
//
//      }
//      GNS.getLogger().severe("New thread.");
//      GNS.getLogger().severe(sb.toString());
//    }
  }
}