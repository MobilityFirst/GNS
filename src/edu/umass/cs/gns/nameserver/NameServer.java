package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.ReplicationFrameworkType;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.recordmap.BasicRecordMap;
import edu.umass.cs.gns.nameserver.replicacontroller.ComputeNewActivesTask;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaController;
import edu.umass.cs.gns.nio.*;
import edu.umass.cs.gns.paxos.PaxosConfig;
import edu.umass.cs.gns.paxos.PaxosManager;
import edu.umass.cs.gns.ping.PingManager;
import edu.umass.cs.gns.ping.PingServer;
import edu.umass.cs.gns.replicationframework.ReplicationFrameworkInterface;
import edu.umass.cs.gns.util.*;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class NameServer {

  /**
   * Name server's id *
   */
  private static int nodeID;
  /**
   * UDP socket over which DNSPackets are received and sent *
   */

  private static BasicRecordMap recordMap;
  private static BasicRecordMap replicaController;
  private static ReplicationFrameworkInterface replicationFramework;
  private static MovingAverage loadMonitor = new MovingAverage(StartNameServer.loadMonitorWindow);

  private static GNSNIOTransportInterface tcpTransport;

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
    GNS.getLogger().info("GNS Version: " + GNS.readBuildVersion());

    GNS.getLogger().info("NS Node " + NameServer.nodeID + " using " + StartNameServer.dataStore.toString() + " data"
            + " store");

    // Executor service created.
    executorService = new ScheduledThreadPoolExecutor(StartNameServer.workerThreadCount);

    initializeDatabase();

    // database must be initialized before creating paxos manager. during recovery process, paxos manager interacts
    // with database
    initializeTransportObjects();

    initializePaxosManager();

    createPrimaryPaxosInstances();

    if (StartNameServer.experimentMode) {
      loadRecordsBeforeExperiments();
    }

    // there is no need to initialize replication framework until we have completed recovery of the existing set of
    // records at this name server.
    initializeReplicationFramework();

    // START ADMIN THREAD - DO NOT REMOVE THIS
    new NSListenerAdmin().start(); // westy

    timer.schedule(new OutputNodeStats(), 100000, 100000); // write stats about system
    GNS.getLogger().info("Ping server started on port " + ConfigFileInfo.getPingPort(nodeID));

    if (StartNameServer.emulatePingLatencies == false) {
    // we emulate latencies based on ping latency given in config file,
    // and do not want ping latency values to be updated
      PingServer.startServerThread(nodeID);
      GNS.getLogger().info("NS Node " + NameServer.getNodeID() + " started Ping server on port " + ConfigFileInfo.getPingPort(nodeID));
      PingManager.startPinging(nodeID);
    }


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

      // Abhigyan: commented this because we are replicating using votes sent by local name servers instead of stats
      // send by actives
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

  /**
   * Creates TCP and UDP transport objects.
   * @throws IOException
   */
  private void initializeTransportObjects() throws IOException {

    // Create the demultiplexer object. This is used by both TCP and UDP.
    nsDemultiplexer = new NSPacketDemultiplexer();

    // Start listening on UDP socket.
    new NSListenerUDP().start();

    // Abhigyan: we are testing with GNSNIOTransport so keeping this code here
    if (StartNameServer.useGNSNIOTransport) {
      JSONMessageWorker worker = new JSONMessageWorker(nsDemultiplexer);
      tcpTransport = new GNSNIOTransport(nodeID, new GNSNodeConfig(), worker);
      if (StartNameServer.emulatePingLatencies) {
        GNSDelayEmulator.emulateConfigFileDelays(StartNameServer.variation);
      }

    } else {
      ByteStreamToJSONObjects worker = new ByteStreamToJSONObjects(nsDemultiplexer);
      NioServer nioServer = new NioServer(nodeID, worker, new GNSNodeConfig());
      if (StartNameServer.emulatePingLatencies) {
        nioServer.emulateConfigFileDelays(StartNameServer.variation);
      }
      tcpTransport = nioServer;
    }

    if (StartNameServer.experimentMode) {
      try {
        Thread.sleep(initialExpDelayMillis); // Abhigyan: wait so that other name servers can bind to respective TCP ports.
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    // start listening socket
    new Thread(tcpTransport).start();

  }

  /**
   * Create the paxos manager object which will recover from logs and start the failure detector.
   */
  private void initializePaxosManager() throws IOException {

    PaxosConfig paxosConfig = new PaxosConfig();
    paxosConfig.setFailureDetectionPingMillis(StartNameServer.failureDetectionPingInterval);
    paxosConfig.setFailureDetectionTimeoutMillis(StartNameServer.failureDetectionTimeoutInterval);
    paxosConfig.setPaxosLogFolder(StartNameServer.paxosLogFolder);
    // Create paxos manager and do log recovery (if logs exist). paxos manager wont send any messages yet.
    paxosManager = new PaxosManager(nodeID, new NameServerNodeConfig(), tcpTransport,
            new NSPaxosInterface(), paxosConfig);

  }

  /**
   * In experiment mode, this will load all name records at the start of the experiment.
   */
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

    GNS.getLogger().info("ComputeNewActives Initial delay " + initialDelayMillis);
    return initialDelayMillis;
  }

  private static void createPrimaryPaxosInstances() {

    HashMap<String, Set<Integer>> groupIDsMembers = ConsistentHashing.getReplicaControllerGroupIDsForNode(nodeID);

    for (String groupID : groupIDsMembers.keySet()) {
      String paxosID = ReplicaController.getPaxosIDForReplicaControllerGroup(groupID);

      GNS.getLogger().info("Creating paxos instances: " + paxosID + "\t" + groupIDsMembers.get(groupID));
      NameServer.paxosManager.createPaxosInstance(paxosID, 0, groupIDsMembers.get(groupID), "");
    }

  }

  /**** End methods for initializing different components of name server ***/

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
      NameServer.tcpTransport.sendToID(recipientId, json);
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
  public static GNSNIOTransportInterface getTcpTransport() {
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