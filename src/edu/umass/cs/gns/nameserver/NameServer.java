package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.database.Field;
import edu.umass.cs.gns.database.FieldType;
import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.recordmap.BasicRecordMap;
import edu.umass.cs.gns.nameserver.replicacontroller.ComputeNewActivesTask;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gns.nio.ByteStreamToJSONObjects;
import edu.umass.cs.gns.nio.NioServer2;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.replicationframework.*;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.MovingAverage;
import edu.umass.cs.gns.util.Util;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Timer;
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
  public static ReplicationFramework replicationFramework;
  public static MovingAverage loadMonitor;
  public static NioServer2 tcpTransport;
  public static Timer timer;
  public static NSPacketDemultiplexer nsDemultiplexer;
  public static ScheduledThreadPoolExecutor executorService;

  /**
   * ***********************************************************
   * Constructs a name server which uses a synthetic workload of integers as names in its record table. The size of the workload is
   * used to generate records and the integer value represents the name and its popularity.
   *
   * @param nodeID Name server id
   * @throws IOException **********************************************************
   */
  public NameServer(int nodeID) throws IOException {
    NameServer.nodeID = nodeID;
    GNS.getLogger().info("NS Node " + NameServer.nodeID + " using " + StartNameServer.dataStore.toString() + " data store");
    // THIS IS WHERE THE NAMESERVER DELEGATES TO THE APPROPRIATE BACKING STORE
    NameServer.recordMap = (BasicRecordMap) Util.createObject(StartNameServer.dataStore.getClassName(),
            // probably should use something more generic here
            MongoRecords.DBNAMERECORD);
    // Ditto for the replica controller records
    NameServer.replicaController = (BasicRecordMap) Util.createObject(StartNameServer.dataStore.getClassName(),
            // probably should use something more generic here
            MongoRecords.DBREPLICACONTROLLER);

    // what type of replication?
    if (StartNameServer.locationBasedReplication) {
      this.replicationFramework = new LocationBasedReplication();
    } else if (StartNameServer.randomReplication) {
      this.replicationFramework = new RandomReplication();
    } else if (StartNameServer.beehiveReplication) {
      BeehiveReplication.generateReplicationLevel(StartNameServer.C,
              StartNameServer.regularWorkloadSize + StartNameServer.mobileWorkloadSize,
              StartNameServer.alpha, StartNameServer.base);
      this.replicationFramework = new RandomReplication();
    } else if (StartNameServer.kmediodsReplication) {
      this.replicationFramework = new KMediods();
    }

    // Timer object created.
    timer = new Timer();

//      ((Thread)timer).setPriority();


    // Executor service created.
    int maxThreads = 5;
    executorService = new ScheduledThreadPoolExecutor(maxThreads);


    // Non-blocking IO created
    nsDemultiplexer = new NSPacketDemultiplexer();
    ByteStreamToJSONObjects worker = new ByteStreamToJSONObjects(nsDemultiplexer);


//    new Thread(worker).start();
//    tcpTransport = new NioServer(nodeID, ConfigFileInfo.getIPAddress(nodeID),
//            ConfigFileInfo.getNSTcpPort(nodeID), worker);
    tcpTransport = new NioServer2(nodeID, worker, new GNSNodeConfig());

    new Thread(tcpTransport).start();
    PaxosManager.initializePaxosManager(ConfigFileInfo.getNumberOfNameServers(), nodeID, tcpTransport, new NSPaxosInterface(), executorService);
    // Load monitoring calculation initalized.
    loadMonitor = new MovingAverage(StartNameServer.loadMonitorWindow);

  }

  public void run() {
    try {

      // start paxos manager first.
      // this will recover state from paxos logs, if it exists


      // Name server starts listening on UDP Port for messages.
//      new NSListenerUDP().start();

      // admin thread started
      new NSListenerAdmin().start(); // westy


      if (StartNameServer.experimentMode) {
        // Name Records added for experiments
//        GenerateSyntheticRecordTable.addNameRecordsToDB(StartNameServer.regularWorkloadSize,StartNameServer.mobileWorkloadSize);
        GenerateSyntheticRecordTable.generateRecordTable(StartNameServer.regularWorkloadSize,
                StartNameServer.mobileWorkloadSize, StartNameServer.defaultTTLRegularName,
                StartNameServer.defaultTTLMobileName);
      }

      // schedule periodic computation of new active name servers.
      if (!(StartNameServer.staticReplication || StartNameServer.optimalReplication)) {

        // Abhigyan: commented this because we are using lns votes instead of stats send by actives to decide replication
        // longer term solution is to integrate geoIPlocation database at name servers.
//        executorService.scheduleAtFixedRate(new SendNameRecordStats(),
//                (new Random()).nextInt((int) StartNameServer.aggregateInterval),
//                StartNameServer.aggregateInterval, TimeUnit.MILLISECONDS);

        executorService.scheduleAtFixedRate(new ComputeNewActivesTask(), StartNameServer.analysisInterval,
                //                (new Random()).nextInt((int) StartNameServer.analysisInterval),
                StartNameServer.analysisInterval, TimeUnit.MILLISECONDS);

        // commenting keep alive messages
//        executorService.scheduleAtFixedRate(new SenderKeepAliveRC(),
//                SenderKeepAliveRC.KEEP_ALIVE_INTERVAL_SEC + (new Random()).nextInt(SenderKeepAliveRC.KEEP_ALIVE_INTERVAL_SEC),
//                SenderKeepAliveRC.KEEP_ALIVE_INTERVAL_SEC, TimeUnit.SECONDS);


      }
    } catch (IOException e) {
      e.printStackTrace();
    }
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
  public static NameRecord getNameRecordMultiField(String name, ArrayList<Field> fields)
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
  public static NameRecord getNameRecordMultiField(String name, ArrayList<Field> fields, ArrayList<Field> userFields)
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
  public static NameRecord getNameRecordMultiField(String name, ArrayList<Field> fields, String... userFieldNames)
          throws RecordNotFoundException {
    return new NameRecord(recordMap.lookup(name, NameRecord.NAME, fields, NameRecord.VALUES_MAP, userFieldList(userFieldNames)));
  }

  private static ArrayList<Field> userFieldList(String... fieldNames) {
    ArrayList<Field> result = new ArrayList<Field>();
    for (String name : fieldNames) {
      result.add(new Field(name, FieldType.LIST_STRING));
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

  public static ReplicaControllerRecord getNameRecordPrimaryMultiField(String name, Field ... fields)
          throws RecordNotFoundException {
    return getNameRecordPrimaryMultiField(name, new ArrayList<Field>(Arrays.asList(fields)));
  }
  /**
   * Read name record with select fields
   * @param name
   * @param fields
   * @return
   * @throws RecordNotFoundException
   */
  public static ReplicaControllerRecord getNameRecordPrimaryMultiField(String name, ArrayList<Field> fields)
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
}
