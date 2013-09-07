package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.fields.Field;
import edu.umass.cs.gns.nameserver.recordExceptions.RecordExistsException;
import edu.umass.cs.gns.nameserver.recordExceptions.RecordNotFoundException;
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
import java.util.Random;
import java.util.Set;
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

//    NameServer.dnsSocket = new DatagramSocket(ConfigFileInfo.getDnsPort(nodeID));
//    NameServer.updateSocket = new DatagramSocket(ConfigFileInfo.getUpdatePort(nodeID));
    GNS.getLogger().info("NS Node " + NameServer.nodeID + " using " + StartNameServer.dataStore.toString() + " data store");
    // THIS IS WHERE THE NAMESERVER DELEGATES TO THE APPROPRIATE BACKING STORE
    NameServer.recordMap = (BasicRecordMap) Util.createObject(StartNameServer.dataStore.getClassName(),
            MongoRecords.DBNAMERECORD);
    // Ditto for the replica controller records
    NameServer.replicaController = (BasicRecordMap) Util.createObject(StartNameServer.dataStore.getClassName(),
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

        executorService.scheduleAtFixedRate(new ComputeNewActivesTask(),
                (new Random()).nextInt((int) StartNameServer.analysisInterval),
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

  //    private void restartAfterCrash() {
//        Set<NameRecord> nameRecords = getAllNameRecords();
//        // check if all name records have replicas created for them.
//        for (NameRecord nameRecord: nameRecords) {
//            if (nameRecord.isPrimaryReplica()) {
//                String primaryPaxosID = ReplicaController.getPrimaryPaxosID(nameRecord);
//                if (!PaxosManager.doesPaxosInstanceExist(primaryPaxosID)) {
//
//                }
//            }
//        }
//    }
//  public static ReplicaControllerRecord getPrimaryNameRecord(String name) {
//    return (ReplicaControllerRecord) recordMap.getNameRecord(name);
//  }
//
//  public static NameRecord getActiveNameRecord(String name) {
//        return (NameRecord)recordMap.getNameRecord(name);
//  }
//    public static NameRecord getNameRecord(String name) {
//        return recordMap.getNameRecord(name);
//    }
//  public static void addNameRecord(NameRecord recordEntry) {
//    if (StartNameServer.debugMode) {
//      GNS.getLogger().fine("Start addNameRecord " + recordEntry.getName());
//    }
//
////    try {
//      // first add to database
//      recordMap.addNameRecord(recordEntry);
//
//      // then create paxos instance
//      // if this node is a primary name server for this name, then create a paxos instance for this name
//      // whose node IDs are the primary name servers
//      HashSet<Integer> primaries = recordEntry.getPrimaryNameservers();
//      if (StartNameServer.debugMode) {
//        GNS.getLogger().fine("Start primary " + primaries);
//      }
//      if (primaries.contains(nodeID)) {
//        if (StartNameServer.debugMode) {
//          GNS.getLogger().fine("Starting replication controller primary" + primaries);
//        }
//        ReplicaController.handleNameRecordAddAtPrimary(recordEntry, primaries);
//      }
////    } catch (JSONException e) {
////      GNS.getLogger().severe("Error getting json record: " + e);
////    }
//
//  }



//
//
//
//
//  /**
//   * Creates a name record that loads reads and writes fields on demand.
//   *
//   * @param name
//   * @return
//   */
//  public static NameRecord getNameRecordLazy(String name) {
//    GNS.getLogger().info("Creating lazy name record for " + name);
//    return recordMap.getNameRecordLazy(name);
//  }
//
//  public static NameRecord getNameRecordLazy(String name, ArrayList<String> keys) {
//    GNS.getLogger().info("Creating lazy name record for " + name);
//    return recordMap.getNameRecordLazy(name, keys);
//  }
//
  /******************************
   * Name Record methods
   ******************************/

  /**
   * Read the complete name record
   * @param name
   * @return
   * @throws RecordNotFoundException
   */
  public static NameRecord getNameRecord(String name) throws RecordNotFoundException{
    return recordMap.getNameRecord(name);
  }

  /**
   * Read name record with select fields
   * @param name
   * @param fields
   * @param userFields
   * @return
   * @throws RecordNotFoundException
   */
  public static NameRecord getNameRecordMultiField(String name, ArrayList<Field> fields, ArrayList<Field> userFields)
          throws RecordNotFoundException{
    return new NameRecord(recordMap.lookup(name, NameRecord.NAME, fields, NameRecord.VALUES_MAP, userFields));
  }

  /**
   * Add this name record to DB
   * @param record
   * @throws RecordExistsException
   */
  public static void addNameRecord(NameRecord record) throws RecordExistsException{
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
   * Return all name records in DB (Expensive operation)
   * @return
   */
  public static Set<NameRecord> getAllNameRecords() {
    return recordMap.getAllNameRecords();
  }


//  public static boolean containsName(String name) {
//    return recordMap.containsName(name);
//  }

//  public static ReplicaControllerRecord getNameRecordPrimaryLazy(String name) {
//    GNS.getLogger().info("Creating lazy primary name record for " + name);
//    return replicaController.getNameRecordPrimaryLazy(name);
//  }




  /******************************
   * Replica controller methods
   ******************************/

  /**
   * Read the complete ReplicaControllerRecord from database
   * @param name
   * @return
   */
  public static ReplicaControllerRecord getNameRecordPrimary(String name) throws RecordNotFoundException{
    return replicaController.getNameRecordPrimary(name);
  }


  /**
   * Read name record with select fields
   * @param name
   * @param fields
   * @return
   * @throws RecordNotFoundException
   */
  public static ReplicaControllerRecord getNameRecordPrimaryMultiField(String name, ArrayList<Field> fields)
          throws RecordNotFoundException{
    return new ReplicaControllerRecord(replicaController.lookup(name, ReplicaControllerRecord.NAME, fields));
  }


  /**
   * Add this record to database
   * @param record
   */
  public static void addNameRecordPrimary(ReplicaControllerRecord record) throws RecordExistsException{
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

  /**
   * Return all name records in DB (Expensive operation)
   * @return
   */
  public static Set<ReplicaControllerRecord> getAllPrimaryNameRecords() {
    return replicaController.getAllPrimaryNameRecords();
  }


  //  the nuclear option
  public static void resetDB() {
    recordMap.reset();
    // reset them both
    replicaController.reset();
  }



  //  public static boolean isActiveNameServer(String name) {
//    //println("isActiveNameServer: recordKey = " + recordKey + " name = " + name, debugMode);
//    //println("isActiveNameServer: " + NameServer.recordMap, debugMode);
//    NameRecord nameRecord = getNameRecord(name);
//    //println("isActiveNameServer: " + nameRecord.toString(), debugMode);
//    return (nameRecord != null) ? nameRecord.containsActiveNameServer(nodeID) : false;
//    //recordMap.getNameRecordField(NameRecord.);
//  }
//  public static Set<Integer> getActiveNameServers(String name) {
//    NameRecord nameRecord = getNameRecord(name);
//    return (nameRecord == null) ? new HashSet<Integer>() : nameRecord.copyActiveNameServers();
//  }
//  public static Set<Integer> getAllNameServers(String name) {
//    NameRecord nameRecord = getNameRecord(name);
//    return (nameRecord == null) ? new HashSet<Integer>() : nameRecord.copyAllNameServers();
//  }
//  public static boolean isPrimaryNameServer(String name) {
//    ReplicaControllerRecord nameRecord = getPrimaryNameRecord(name);
//    //println("isPrimaryNameServer: " + nameRecord.toString(), debugMode);
//    return (nameRecord != null) ? nameRecord.containsPrimaryNameserver(nodeID) : false;
//  }
//  public static Set<Integer> getPrimaryNameServers(String name) {
//    NameRecord nameRecord = getNameRecord(name);
//
//    if (nameRecord != null) {
//      return nameRecord.getPrimaryNameservers();
//    } else {
//      return getPrimaryReplicas(name);
//    }
//
//  }
//  public static int getWriteFrequency(String name) {
//    NameRecord nameRecord = getNameRecord(name);
//    return (nameRecord == null) ? 0 : nameRecord.getTotalWriteFrequency();
//  }
//
//  public static int getReadFrequency(String name) {
//    NameRecord nameRecord = getNameRecord(name);
//    return (nameRecord == null) ? 0 : nameRecord.getTotalReadFrequency();
//  }
//  public static Set<NameRecord> getAllNameRecords() {
//    return recordMap.getAllNameRecords();
//  }
//    // ** NEW **
//    public static Set<NameRecord> getAllPrimaryNameRecords() {
//        return recordMap.getAllNameRecords();
//    }
//    public static Set<NameRecord> getAllActiveNameRecords() {
//        return recordMap.getAllNameRecords();
//    }


}
