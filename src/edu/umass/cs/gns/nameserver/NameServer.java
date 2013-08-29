package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.recordmap.BasicRecordMap;
import edu.umass.cs.gns.nameserver.replicacontroller.ComputeNewActivesTask;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gns.nio.ByteStreamToJSONObjects;
import edu.umass.cs.gns.nio.NioServer2;
import edu.umass.cs.gns.packet.DNSPacket;
import edu.umass.cs.gns.packet.DNSRecordType;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.replicationframework.*;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.MovingAverage;
import edu.umass.cs.gns.util.Util;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
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
  private static BasicRecordMap recordMap;
  private static BasicRecordMap replicaController;
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
//
//    // THIS IS WHERE THE NAMESERVER DELEGATES TO THE APPROPRIATE BACKING STORE
//    NameServer.recordMap = new MongoRecordMap(MongoRecords.DBNAMERECORD);
//    // Ditto for the replica controller records
//    NameServer.replicaController = new MongoRecordMap(MongoRecords.DBREPLICACONTROLLER);

    // THIS IS WHERE THE NAMESERVER DELEGATES TO THE APPROPRIATE BACKING STORE
    //NameServer.recordMap = new CassandraRecordMap(CassandraRecords.DBNAMERECORD);
    // Ditto for the replica controller records
    //NameServer.replicaController = new CassandraRecordMap(CassandraRecords.DBREPLICACONTROLLER);
    

    // will need to add back some form of the code to select the appropriate one
    // probably make persistentDataStore a selector

//    if (StartNameServer.persistentDataStore) {
//      if (StartNameServer.debugMode) {
//        GNS.getLogger().info("NS Node " + NameServer.nodeID + " using Mongo DB ");
//      }
//      //throw new UnsupportedOperationException("MongoRecordMap is not supported yet.");
//      NameServer.recordMap = new MongoRecordMap();
//      //NameServer.recordMap = new InCoreRecordMapJSON();
//
////      if (StartNameServer.debugMode) GNRS.getLogger().info("NS Node " + NameServer.nodeID + " using Cassandra DB ");
////      NameServer.recordMap = new CassandraRecordMap();
//    } else if (StartNameServer.simpleDiskStore) {
//      throw new UnsupportedOperationException("InCoreWithDiskBackup is not supported yet.");
////      if (StartNameServer.debugMode) {
////        GNRS.getLogger().info("NS Node " + NameServer.nodeID + " using Simple Disk Store DB ");
////      }
////      NameServer.recordMap = new InCoreWithDiskBackupV1();
//    } else {
//      //throw new UnsupportedOperationException("InCoreRecordMap is not supported yet.");
//      if (StartNameServer.debugMode) {
//        GNS.getLogger().info("NS Node " + NameServer.nodeID + " using In Core DB ");
//      }
//      NameServer.recordMap = new InCoreRecordMapJSON();
//    }


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

    executorService =
            new ScheduledThreadPoolExecutor(maxThreads);

    // Non-blocking IO created
    nsDemultiplexer = new NSPacketDemultiplexer();
    ByteStreamToJSONObjects worker = new ByteStreamToJSONObjects(nsDemultiplexer);
//    new Thread(worker).start();
//    tcpTransport = new NioServer(nodeID, ConfigFileInfo.getIPAddress(nodeID),
//            ConfigFileInfo.getNSTcpPort(nodeID), worker);
    tcpTransport = new NioServer2(nodeID, worker, new NSNodeConfig());

    new Thread(tcpTransport).start();

    // Load monitoring calculation initalized.
    loadMonitor = new MovingAverage(StartNameServer.loadMonitorWindow);

  }

  public void run() {
    try {

      // start paxos manager first.
      // this will recover state from paxos logs, if it exists
      PaxosManager.initializePaxosManager(ConfigFileInfo.getNumberOfNameServers(), nodeID, tcpTransport, new NSPaxosInterface(), executorService);

      // Name server starts listening on UDP Port for messages.
//      new NSListenerUDP().start();

      // admin thread started
      new NSListenerAdmin().start(); // westy


      if (StartNameServer.experimentMode) {
        // Name Records added for experiments
        GenerateSyntheticRecordTable.generateRecordTable(StartNameServer.regularWorkloadSize,
                StartNameServer.mobileWorkloadSize, StartNameServer.defaultTTLRegularName,
                StartNameServer.defaultTTLMobileName);
      }

      // schedule periodic computation of new active name servers.
      if (!(StartNameServer.staticReplication || StartNameServer.optimalReplication)) {

        executorService.scheduleAtFixedRate(new SendNameRecordStats(),
                (new Random()).nextInt((int) StartNameServer.aggregateInterval),
                StartNameServer.aggregateInterval, TimeUnit.MILLISECONDS);

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
    } catch (NoSuchAlgorithmException e) {
      GNS.getLogger().severe("Error generating synthetic data " + e);
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
  // THIS IS WHERE THE NAMESERVER DELEGATES TO THE APPROPRIATE BACKING STORE
  public static NameRecord getNameRecord(String name) {
    return recordMap.getNameRecord(name);
  }

  /**
   * Creates a name record that loads reads and writes fields on demand.
   *
   * @param name
   * @return
   */
  public static NameRecord getNameRecordLazy(String name) {
    GNS.getLogger().info("Creating lazy name record for " + name);
    return recordMap.getNameRecordLazy(name);
  }

  public static NameRecord getNameRecordLazy(String name, ArrayList<String> keys) {
    GNS.getLogger().info("Creating lazy name record for " + name);
    return recordMap.getNameRecordLazy(name, keys);
  }

  public static void addNameRecord(NameRecord record) {
    recordMap.addNameRecord(record);
  }

  public static void updateNameRecord(NameRecord record) {
    if (!record.isLazyEval()) {
      recordMap.updateNameRecord(record);
    }
  }

  public static void removeNameRecord(String name) {
    recordMap.removeNameRecord(name);
  }


  public static boolean containsName(String name) {
    return recordMap.containsName(name);
  }

  public static Set<NameRecord> getAllNameRecords() {
    return recordMap.getAllNameRecords();
  }

  public static ReplicaControllerRecord getNameRecordPrimaryLazy(String name) {
    GNS.getLogger().info("Creating lazy primary name record for " + name);
    return replicaController.getNameRecordPrimaryLazy(name);
  }

  public static ReplicaControllerRecord getNameRecordPrimary(String name) {
    return replicaController.getNameRecordPrimary(name);
  }

  public static void addNameRecordPrimary(ReplicaControllerRecord record) {
    replicaController.addNameRecordPrimary(record);
  }

  public static void removeNameRecordPrimary(String name) {
    replicaController.removeNameRecord(name);
  }


  public static void updateNameRecordPrimary(ReplicaControllerRecord record) {
    if (!record.isLazyEval()) {
      replicaController.updateNameRecordPrimary(record);
    }
  }

  public static Set<ReplicaControllerRecord> getAllPrimaryNameRecords() {
    return replicaController.getAllPrimaryNameRecords();
  }

  //  the nuclear option
  public static void resetDB() {
    recordMap.reset();
    // reset them both
    replicaController.reset();
  }
  
  //  the nuclear option
  public static void deleteAllDatabases() {
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
  public static DNSPacket makeResponseFromRecord(DNSPacket dnsPacket, NameRecord nameRecord) {
    if (dnsPacket.getQname() == null || !dnsPacket.isQuery() //|| !DBNameRecord.containsName(dnsPacket.getQname())
            // shouldn't be called with a null namerecord, but just to be sure
            || nameRecord == null || !nameRecord.containsKey(dnsPacket.getQrecordKey().getName())) {
      dnsPacket.getHeader().setRcode(DNSRecordType.RCODE_ERROR);
      dnsPacket.getHeader().setQr(1);
      return dnsPacket;
    }

    //NameRecord nameRecord = getNameRecord(dnsPacket.qname, dnsPacket.qrecordKey);
    //Generate the respose packet
    dnsPacket.getHeader().setRcode(DNSRecordType.RCODE_NO_ERROR);
    dnsPacket.getHeader().setQr(1);
    dnsPacket.setTTL(nameRecord.getTimeToLive());
//    dnsPacket.setRecordValue(nameRecord.getValuesMap());
    // this is redundant with above, but for now we keep both
    dnsPacket.setFieldValue(nameRecord.get(dnsPacket.getQrecordKey().getName()));
//    dnsPacket.setPrimaryNameServers(nameRecord.getPrimaryNameservers());
//    dnsPacket.setActiveNameServers(nameRecord.copyActiveNameServers());

    //update lookup frequency
    nameRecord.incrementLookupRequest();
    return dnsPacket;
  }
//  public static String tableToString() {
//    return recordMap.tableToString();
//  }
}
