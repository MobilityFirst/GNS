package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.ReplicationFrameworkType;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.recordmap.BasicRecordMap;
import edu.umass.cs.gns.nameserver.replicacontroller.ComputeNewActivesTask;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaController;
import edu.umass.cs.gns.nio.ByteStreamToJSONObjects;
import edu.umass.cs.gns.nio.NioServer;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.ping.PingServer;
import edu.umass.cs.gns.ping.Pinger;
import edu.umass.cs.gns.replicationframework.ReplicationFrameworkInterface;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.ConsistentHashing;
import edu.umass.cs.gns.util.MovingAverage;
import edu.umass.cs.gns.util.OutputMemoryUse;
import edu.umass.cs.gns.util.Util;
import java.io.IOException;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;

public class NameServer {

  /**
   * Nameserver's id *
   */
  private static int nodeID;
  /**
   * UDP socket over which DNSPackets are received and sent *
   */
  private static BasicRecordMap recordMap;
  private static BasicRecordMap replicaController;
  private static ReplicationFrameworkInterface replicationFramework;
  private static MovingAverage loadMonitor = new MovingAverage(StartNameServer.loadMonitorWindow);
  private static NioServer tcpTransport;
//  public static GNSNIOTransport tcpTransport; // Abhigyan: we are testing with GNSNIOTransport so keeping this field here
  private static NSPacketDemultiplexer nsDemultiplexer;
  private static Timer timer = new Timer();
  private static ScheduledThreadPoolExecutor executorService;
  private static PaxosManager paxosManager;
  /**
   * Only used during experiments.
   */
  private static int initialExpDelayMillis = 30000;

  /**
   * Call this constructor only after parsing all the options given in config file or command line.
   *
   * Constructs a name server which uses a synthetic workload of integers as names in its record table. The size of the
   * workload is used to generate records and the integer value represents the name and its popularity.
   *
   * @param nodeID Name server id
   * @throws IOException
   */
  public NameServer(int nodeID) throws IOException {

    NameServer.nodeID = nodeID;
    GNS.getLogger().info("GNS Version: " + GNS.readBuildVersion() + "\n");

    GNS.getLogger().info("NS Node " + NameServer.nodeID + " using " + StartNameServer.dataStore.toString() + " data"
            + " store");

    // Executor service created.
    executorService = new ScheduledThreadPoolExecutor(StartNameServer.workerThreadCount);

    initializeDatabase();

    // database must be initialized before creating paxos manager. during recovery process, paxos manager interacts
    // with database
    initializeTransportObjectsAndPaxosManager();

    if (StartNameServer.experimentMode) {
      loadRecordsBeforeExperiments();
    }

    // there is no need to initialize replication framework until we have completed recovery of the existing set of
    // records at this name server.
    initializeReplicationFramework();

    // START ADMIN THREAD - DO NOT REMOVE THIS
    new NSListenerAdmin().start(); // westy

    PingServer.startServerThread(nodeID);
    GNS.getLogger().info("NS Node " + NameServer.getNodeID() + " started Ping server on port " + ConfigFileInfo.getPingPort(nodeID));
    Pinger.startPinging(nodeID);

    timer.schedule(new OutputMemoryUse(), 100000, 100000); // write stats about system

  }

  /**** Begin methods for initializing different components of name server ***/
  private void initializeDatabase() {
    // THIS IS WHERE THE NAMESERVER DELEGATES TO THE APPROPRIATE BACKING STORE
    NameServer.recordMap = (BasicRecordMap) Util.createObject(StartNameServer.dataStore.getClassName(),
            // probably should use something more generic here
            MongoRecords.DBNAMERECORD);
    // Ditto for the replica controller records
    NameServer.replicaController = (BasicRecordMap) Util.createObject(StartNameServer.dataStore.getClassName(),
            // probably should use something more generic here
            MongoRecords.DBREPLICACONTROLLER);
    // clear out the data base - 
    // this is being done because paxos will reinsert all the records in the database
    resetDB();
  }

  private void initializeReplicationFramework() {
    NameServer.replicationFramework = ReplicationFrameworkType.instantiateReplicationFramework(
            StartNameServer.replicationFramework);
    // schedule periodic computation of new active name servers.
    if (!(StartNameServer.replicationFramework == ReplicationFrameworkType.STATIC
            || StartNameServer.replicationFramework == ReplicationFrameworkType.OPTIMAL)) {

      // Abhigyan: commented this because we are using lns votes instead of stats send by actives to decide replication
      // TODO longer term solution is to integrate IP geo-location database at name servers.
//        executorService.scheduleAtFixedRate(new SendNameRecordStats(),
//                (new Random()).nextInt((int) StartNameServer.aggregateInterval),
//                StartNameServer.aggregateInterval, TimeUnit.MILLISECONDS);

      executorService.scheduleAtFixedRate(new ComputeNewActivesTask(), getInitialDelayForComputingNewActives(),
              StartNameServer.analysisInterval, TimeUnit.MILLISECONDS);

      //  commenting keep alive messages
//        executorService.scheduleAtFixedRate(new SenderKeepAliveRC(),
//                SenderKeepAliveRC.KEEP_ALIVE_INTERVAL_SEC + (new Random()).nextInt(SenderKeepAliveRC.KEEP_ALIVE_INTERVAL_SEC),
//                SenderKeepAliveRC.KEEP_ALIVE_INTERVAL_SEC, TimeUnit.SECONDS);
    }
  }

  private void initializeTransportObjectsAndPaxosManager() throws IOException {

    // Create the demultiplexer object. This is used by both TCP and UDP.
    nsDemultiplexer = new NSPacketDemultiplexer();

    // Start listening on UDP socket.
    new NSListenerUDP().start();

    // create a TCP transport object because we need to pass it to paxos manager.
    // Don't start the listening socket because paxos manager is not initialized yet. Another reason is that
    // we are still doing log recovery and don't want to process new messages.

    ByteStreamToJSONObjects worker = new ByteStreamToJSONObjects(nsDemultiplexer);
    tcpTransport = new NioServer(nodeID, worker, new GNSNodeConfig());

    // Abhigyan: we are testing with GNSNIOTransport so keeping this code here
//    JSONMessageWorker worker = new JSONMessageWorker(nsDemultiplexer);
//    tcpTransport = new GNSNIOTransport(nodeID, new GNSNodeConfig(), worker);

    // Create paxos manager and do log recovery (if logs exist). paxos manager wont send any messages yet.
    paxosManager = new PaxosManager(ConfigFileInfo.getNumberOfNameServers(), nodeID, tcpTransport,
            new NSPaxosInterface(), executorService, StartNameServer.paxosLogFolder);

    // start listening socket
    new Thread(tcpTransport).start();

    if (StartNameServer.experimentMode) {
      try {
        Thread.sleep(initialExpDelayMillis); // Abhigyan: wait so that other name servers can bind to respective TCP ports.
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    // now listening socket is running, start failure detector and other process in paxos which require sending messages.
    paxosManager.startPaxos(StartNameServer.failureDetectionPingInterval, StartNameServer.failureDetectionTimeoutInterval);

    createPrimaryPaxosInstances();
  }

  private void loadRecordsBeforeExperiments() {
    if (StartNameServer.experimentMode) {

      if (StartNameServer.nameActives == null) {
        GenerateSyntheticRecordTable.generateRecordTableBulkInsert(StartNameServer.regularWorkloadSize,
                StartNameServer.mobileWorkloadSize, StartNameServer.TTLRegularName,
                StartNameServer.TTLMobileName);
      } else {
        GenerateSyntheticRecordTable.generateRecordTableWithActivesNew(StartNameServer.nameActives);
      }
    }

  }

  private long getInitialDelayForComputingNewActives() {
    long initialDelayMillis = initialExpDelayMillis + StartNameServer.analysisInterval + // wait for one interval for estimating demand
            (new Random()).nextInt((int) StartNameServer.analysisInterval); // randomize to avoid synchronization among replicas.
    if (StartNameServer.experimentMode && StartNameServer.quitAfterTimeSec > 0) {
      initialDelayMillis = initialExpDelayMillis + (long) (StartNameServer.analysisInterval * 1.5);

    }
    GNS.getLogger().info("ComputeNewActives Initial delay " + initialDelayMillis);
    return initialDelayMillis;
  }

  public static void createPrimaryPaxosInstances() {

    HashMap<String, Set<Integer>> groupIDsMembers = ConsistentHashing.getReplicaControllerGroupIDsForNode(nodeID);

    for (String groupID : groupIDsMembers.keySet()) {
      String paxosID = ReplicaController.getPaxosIDForReplicaControllerGroup(groupID);

      GNS.getLogger().info("Creating paxos instances: " + paxosID + "\t" + groupIDsMembers.get(groupID));
      NameServer.paxosManager.createPaxosInstance(paxosID, groupIDsMembers.get(groupID), "");
    }

  }

  /**** End methods for initializing different components of name server ***/
//
//  /******************************
//   * Name Record methods
//   ******************************/
//  /**
//   * Load a name record from the backing database and retrieve all the fields.
//   * @param name
//   * @return
//   * @throws RecordNotFoundException
//   */
//  public static NameRecord getNameRecord(String name) throws RecordNotFoundException {
//    return recordMap.getNameRecord(name);
//  }
//
//  /**
//   * Load a name record from the backing database and retrieve certain fields as well.
//   *
//   * @param name
//   * @param systemFields - a list of Field structures representing "system" fields to retrieve
//   * @return
//   * @throws RecordNotFoundException
//   */
//  public static NameRecord getNameRecordMultiField(String name, ArrayList<ColumnField> systemFields)
//          throws RecordNotFoundException {
//    return new NameRecord(recordMap.lookup(name, NameRecord.NAME, systemFields, NameRecord.VALUES_MAP, null));
//  }
//
//  /**
//   * Load a name record from the backing database and retrieve certain fields as well.
//   *
//   * @param name
//   * @param systemFields - a list of Field structures representing "system" fields to retrieve
//   * @param userFields - a list of Field structures representing user fields to retrieve
//   * @return
//   * @throws RecordNotFoundException
//   */
//  public static NameRecord getNameRecordMultiField(String name, ArrayList<ColumnField> systemFields, ArrayList<ColumnField> userFields)
//          throws RecordNotFoundException {
//    return new NameRecord(recordMap.lookup(name, NameRecord.NAME, systemFields, NameRecord.VALUES_MAP, userFields));
//  }
//
//  /**
//   * Load a name record from the backing database and retrieve certain fields as well.
//   *
//   * @param name
//   * @param systemFields
//   * @param userFieldNames - strings which name the user fields to return
//   * @return
//   * @throws RecordNotFoundException
//   */
//  public static NameRecord getNameRecordMultiField(String name, ArrayList<ColumnField> systemFields, String... userFieldNames)
//          throws RecordNotFoundException {
//    return new NameRecord(recordMap.lookup(name, NameRecord.NAME, systemFields, NameRecord.VALUES_MAP, userFieldList(userFieldNames)));
//  }
//
//  private static ArrayList<ColumnField> userFieldList(String... fieldNames) {
//    ArrayList<ColumnField> result = new ArrayList<ColumnField>();
//    for (String name : fieldNames) {
//      result.add(new ColumnField(name, ColumnFieldType.LIST_STRING));
//    }
//    return result;
//  }
//
//  /**
//   * Add this name record to DB
//   * @param record
//   * @throws RecordExistsException
//   */
//  public static void addNameRecord(NameRecord record) throws RecordExistsException {
//    recordMap.addNameRecord(record);
//  }
//
//  /**
//   * Replace the name record in DB with this copy of name record
//   * @param record
//   */
//  public static void updateNameRecord(NameRecord record) {
//    recordMap.updateNameRecord(record);
//  }
//
//  /**
//   * Remove name record from DB
//   * @param name
//   */
//  public static void removeNameRecord(String name) {
//    recordMap.removeNameRecord(name);
//  }
//
//  /**
//   * Returns an iterator for all the rows in the collection with all fields filled in.
//   *
//   * @return
//   */
//  public static BasicRecordCursor getAllRowsIterator() {
//    return recordMap.getAllRowsIterator();
//  }
//
//  /**
//   * Given a key and a value return all the records as a BasicRecordCursor that have a *user* key with that value.
//   * @param key
//   * @param value
//   * @return
//   */
//  public static BasicRecordCursor selectRecords(String key, Object value) {
//    return recordMap.selectRecords(NameRecord.VALUES_MAP, key, value);
//  }
//
//  /**
//   * If key is a GeoSpatial field return all fields that are within value which is a bounding box specified as a nested JSONArray
//   * string tuple of paired tuples: [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]] The returned value is a BasicRecordCursor.
//   *
//   * @param key
//   * @param value - a string that looks like this: [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]]
//   * @return
//   */
//  public static BasicRecordCursor selectRecordsWithin(String key, String value) {
//    return recordMap.selectRecordsWithin(NameRecord.VALUES_MAP, key, value);
//  }
//
//  /**
//   * If key is a GeoSpatial field return all fields that are near value which is a point specified as a JSONArray string tuple:
//   * [LONG, LAT]. maxDistance is in meters. The returned value is a BasicRecordCursor.
//   *
//   * @param key
//   * @param value - a string that looks like this: [LONG, LAT]
//   * @param maxDistance - the distance in meters
//   * @return
//   */
//  public static BasicRecordCursor selectRecordsNear(String key, String value, Double maxDistance) {
//    return recordMap.selectRecordsNear(NameRecord.VALUES_MAP, key, value, maxDistance);
//  }
//
//  /**
//   * Returns all fields that match the query.
//   *
//   * @param query
//   * @return
//   */
//  public static BasicRecordCursor selectRecordsQuery(String query) {
//    return recordMap.selectRecordsQuery(NameRecord.VALUES_MAP, query);
//  }
//
//  /******************************
//   * End of name record methods
//   ******************************/
//  ABHIGYAN: keeping this code commented here because we haven't tested with other code.
//  /******************************
//   * Replica controller methods
//   ******************************/
//  /**
//   * Read the complete ReplicaControllerRecord from database
//   * @param name
//   * @return
//   */
//  public static ReplicaControllerRecord getNameRecordPrimary(String name) throws RecordNotFoundException {
//    return replicaController.getNameRecordPrimary(name);
//  }
//
//  public static ReplicaControllerRecord getNameRecordPrimaryMultiField(String name, ColumnField... fields)
//          throws RecordNotFoundException {
//    return getNameRecordPrimaryMultiField(name, new ArrayList<ColumnField>(Arrays.asList(fields)));
//  }
//
//  /**
//   * Read name record with select fields
//   * @param name
//   * @param fields
//   * @return
//   * @throws RecordNotFoundException
//   */
//  public static ReplicaControllerRecord getNameRecordPrimaryMultiField(String name, ArrayList<ColumnField> fields)
//          throws RecordNotFoundException {
//    return new ReplicaControllerRecord(replicaController.lookup(name, ReplicaControllerRecord.NAME, fields));
//  }
//
//  /**
//   * Add this record to database
//   * @param record
//   */
//  public static void addNameRecordPrimary(ReplicaControllerRecord record) throws RecordExistsException {
//    replicaController.addNameRecordPrimary(record);
//  }
//
//  /**
//   * Remove a ReplicaControllerRecord with given name from database
//   * @param name
//   */
//  public static void removeNameRecordPrimary(String name) {
//    replicaController.removeNameRecord(name);
//  }
//
//  /**
//   * Replace the ReplicaControllerRecord in DB with this copy of ReplicaControllerRecord
//   * @param record
//   */
//  public static void updateNameRecordPrimary(ReplicaControllerRecord record) {
//    replicaController.updateNameRecordPrimary(record);
//  }
//
//  public static BasicRecordCursor getAllPrimaryRowsIterator() {
//    return replicaController.getAllRowsIterator();
//  }
  /**
   * Clears the database and reinitializes all indices.
   * DONT CALL THIS UNLESS YOU WANT TO CLEAR ALL THE RECORDS OUT OF THE DATABASE!!!
   */
  public static void resetDB() {
    recordMap.reset();
    // reset them both
    replicaController.reset();
  }

  /******************************
   * End of Replica controller methods
   ******************************/
  /*** other methods */
  /**
   * Wrapper method to send to LNS
   * @param json  json object to send
   * @param recipientId   node we're sending this to
   */
  public static void returnToSender(JSONObject json, int recipientId) {
    try {
      NameServer.tcpTransport.sendToIDActual(recipientId, json);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * @return the nodeID
   */
  public static int getNodeID() {
    return nodeID;
  }

  /**
   * THIS SHOULD ONLY BE USED IN TEST CODE!!!
   * @param aNodeID the nodeID to set
   */
  public static void setNodeID(int aNodeID) {
    nodeID = aNodeID;
  }

  /**
   * @return the recordMap
   */
  public static BasicRecordMap getRecordMap() {
    return recordMap;
  }

  /**
   * @return the replicaController
   */
  public static BasicRecordMap getReplicaController() {
    return replicaController;
  }

  /**
   * @return the replicationFramework
   */
  public static ReplicationFrameworkInterface getReplicationFramework() {
    return replicationFramework;
  }

  /**
   * @return the loadMonitor
   */
  public static MovingAverage getLoadMonitor() {
    return loadMonitor;
  }

  /**
   * @return the tcpTransport
   */
  public static NioServer getTcpTransport() {
    return tcpTransport;
  }

  /**
   * @return the nsDemultiplexer
   */
  public static NSPacketDemultiplexer getNsDemultiplexer() {
    return nsDemultiplexer;
  }

  /**
   * @return the timer
   */
  public static Timer getTimer() {
    return timer;
  }

  /**
   * @return the executorService
   */
  public static ScheduledThreadPoolExecutor getExecutorService() {
    return executorService;
  }

  /**
   * @return the paxosManager
   */
  public static PaxosManager getPaxosManager() {
    return paxosManager;
  }

  /**
   * @return the initialExpDelayMillis
   */
  public static int getInitialExpDelayMillis() {
    return initialExpDelayMillis;
  }
}