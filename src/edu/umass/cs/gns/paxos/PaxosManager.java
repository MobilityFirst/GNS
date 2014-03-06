package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nio.*;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.paxospacket.FailureDetectionPacket;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import edu.umass.cs.gns.packet.paxospacket.StatePacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Manages a set of Paxos instances, each instance could be running 
 * on a different subset of nodes but including this node.
 * @author abhigyan
 *
 */
public class PaxosManager {

  static final String PAXOS_ID = "PXS";

  /**
   * Total number of nodes. node IDs = 0, 1, ..., N -1
   */
  private  int N;

  /**
   * nodeID of this node. Among node IDs = 0, 1, ..., (N - 1).
   */
  int nodeID;

  /**
   * When paxos is run independently {@code nioServer} is used to send messages between paxos replicas and client.
   */
  private  NioServer nioServer;

  /**
   * Object stores all paxos instances.
   *
   * V. IMP.: The paxosID of a  {@code PaxosReplica} is stored in its field 'paxosID'. The key that this map uses for a
   * {@code PaxosReplica} is given by the method {@code getPaxosKeyFromPaxosID}.
   *
   * The paxosID for the paxos between primaries is defined as 'name-P', its key in this map is also 'name-P'.
   * The paxosID for the paxos between actives is defined as 'name-X', where X is an int. Its key in this map is just
   * 'name'. X changes every time the set of active replicas change. Defining the keys in this manner allows us to find
   * the paxos instance among actives for a name, without knowing what the current 'X' is. Otherwise proposing a
   * request would have needed an additional database lookup to find what the current X is for this name.
   *
   */
  ConcurrentHashMap<String, PaxosReplicaInterface> paxosInstances = new ConcurrentHashMap<String, PaxosReplicaInterface>();

  PaxosInterface clientRequestHandler;

  ScheduledThreadPoolExecutor executorService;

  /**
   *
   */
  final TreeSet<ProposalStateAtCoordinator> proposalStates = new TreeSet<ProposalStateAtCoordinator>();

  /**
   * Paxos logs are garbage collected at this interval
   */
  private static long PAXOS_LOG_STATE_INTERVAL_SEC = 100000;

  /**
   * Redundant paxos logs are checked and deleted at this interval.
   */
  private static long PAXOS_LOG_DELETE_INTERVAL_SEC = 100000;

  /**
   * Minimum interval (in milliseconds) between two garbage state collections of replicas.
   */
  static int MEMORY_GARBAGE_COLLECTION_INTERVAL = 100;

  /**
   * Paxos coordinator checks whether all replicas have received the latest messages decided.
   */
  static long RESEND_PENDING_MSG_INTERVAL_MILLIS = 2000;


  /**
   * debug = true is used to debug the paxos module,  debug = false when complete GNRS system is running.
   */
  private  boolean debug = false;

  PaxosLogger paxosLogger;

  FailureDetection failureDetection;


  /********************BEGIN: public methods for paxos manager********************/

  public PaxosManager(int numberOfNodes, int nodeID, NioServer nioServer, PaxosInterface outputHandler,
                      ScheduledThreadPoolExecutor executorService, String paxosLogFolder) {
    this.N = numberOfNodes;
    this.nodeID = nodeID;
    this.nioServer = nioServer;

    this.clientRequestHandler = outputHandler;

    this.executorService = executorService;

    // recover previous state if exists using logger
    paxosLogger = new PaxosLogger(paxosLogFolder, nodeID, this);
    long t0 = System.currentTimeMillis();
    ConcurrentHashMap<String, PaxosReplicaInterface> myPaxosInstances = paxosLogger.recoverPaxosLogs();

    long t1 = System.currentTimeMillis();
    GNS.getLogger().info("Time to recover paxos logs ... " + (t1 - t0)/1000 + " seconds");
    if (myPaxosInstances != null) paxosInstances = myPaxosInstances;
    paxosLogger.start();
    if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instances: " + paxosInstances.size());

//    if (debug && paxosInstances.size() == 0) createTestPaxosInstance();


    GNS.getLogger().info("Paxos manager initialization complete");

  }

  public void startPaxos(int failureDetectionPing, int failureDetectionTimeout) {

    failureDetection = new FailureDetection(N, nodeID, executorService, this, failureDetectionPing,
            failureDetectionTimeout);

    //
    startAllPaxosReplicas();
    startPaxosMaintenanceActions();
  }
  /**
   * Constructor used during testing only.
   * @param testConfigFile config file used for tests
   * @param nodeID ID of this node
   */
  public PaxosManager(String testConfigFile, int nodeID) {

//    debug = true;
    TestConfig testConfig1= new TestConfig(testConfigFile);
    this.N = testConfig1.numPaxosReplicas;
    this.nodeID = nodeID;

    this.nioServer =  initTransport(testConfig1.numPaxosReplicas + 1,  testConfig1.startingPort);

    clientRequestHandler = new DefaultPaxosInterface(nodeID, nioServer);

    this.executorService = new ScheduledThreadPoolExecutor(testConfig1.maxThreads);
    failureDetection = new FailureDetection(N, nodeID, executorService, this, 1000000, 3000000);

    String paxosLogFolder = testConfig1.testPaxosLogFolder + "/node" + nodeID;
    paxosLogger = new PaxosLogger(paxosLogFolder, nodeID, this);
//    ConcurrentHashMap<String, PaxosReplicaInterface> myPaxosInstances = paxosLogger.recoverPaxosLogs();
    paxosLogger.clearLogs();
//    if (myPaxosInstances != null) paxosInstances = myPaxosInstances;
    paxosLogger.start();

    createTestPaxosInstance(testConfig1.testPaxosID);

    startPaxosMaintenanceActions();
  }

  /**
   * Adds a new Paxos instance to the set of actives.
   */
  public  boolean createPaxosInstance(String paxosID, Set<Integer> nodeIDs, String initialState) {

    if(StartNameServer.debugMode) GNS.getLogger().info(paxosID + "\tEnter createPaxos");
    if (nodeIDs.size() < 3) {
      GNS.getLogger().severe(nodeID + " less than three replicas " +
              "paxos instance cannot be created. SEVERE ERROR. EXCEPTION Exception.");
      return false;
    }

    if (!nodeIDs.contains(nodeID)) {
      GNS.getLogger().severe(nodeID + " this node not a member of paxos instance. replica not created.");
      return false;
    }

    PaxosReplicaInterface r;

    PaxosReplicaInterface r1;
    synchronized (paxosInstances) { // paxosInstance object can be concurrently modified.
      r1 = paxosInstances.get(getPaxosKeyFromPaxosID(paxosID));

      if (r1 != null && r1.getPaxosID().equals(paxosID)) {
        GNS.getLogger().warning("Paxos replica already exists .. " + paxosID);
        return false;

      } else {

        assert initialState != null;
        r = createPaxosReplicaObject(paxosID, nodeID, nodeIDs);//new PaxosReplicaInterface(paxosID, nodeID, nodeIDs);
        if (!StartNameServer.experimentMode) { // During experiments, we disable state logging. This helps us load records faster into database.
          paxosLogger.logPaxosStart(paxosID, nodeIDs, new StatePacket(r.getAcceptorBallot(), 0, initialState));
        }
        if(StartNameServer.debugMode) GNS.getLogger().info(paxosID + "\tBefore creating replica.");
        paxosInstances.put(getPaxosKeyFromPaxosID(paxosID), r);
      }
    }

    if (r1 != null && !r1.getPaxosID().equals(paxosID)) {
      r1.removePendingProposals();
      GNS.getLogger().info("OldPaxos replica replaced .. so log a stop message .. " + r1.getPaxosID() + " new replica " + paxosID);
      paxosLogger.logPaxosStop(r1.getPaxosID());    // multiple stop msgs can get logged because other replica might stop in meanwhile.
    }

    if (r!= null) {
      r.checkCoordinatorFailure(); // propose new ballot if default coordinator has failed
    }
    return true;
  }

  public  void resetAll() {
    // delete paxos instances
    paxosInstances.clear();
    // clear paxos logs
    paxosLogger.clearLogs();
    // run java gc
  }

  /**
   *
   */
  void startResendPendingMessages() {
    ResendPendingMessagesTask task = new ResendPendingMessagesTask(this);
    // single time execution
    executorService.schedule(task, RESEND_PENDING_MSG_INTERVAL_MILLIS,TimeUnit.MILLISECONDS);
  }

  PaxosReplicaInterface createPaxosReplicaObject(String paxosID, int nodeID, Set<Integer> nodeIDs1) {
    return new PaxosReplica(paxosID, nodeID, nodeIDs1, this);
  }

  /**
   * Propose requestPacket in the paxos instance with paxosID.
   * ReqeustPacket.clientID is used to distinguish which method proposed this value.
   * @param paxosID paxosID of the paxos group
   * @param requestPacket request to be proposed
   */
  public  String propose(String paxosID, RequestPacket requestPacket) {

    if (!debug) { // running with GNS
      PaxosReplicaInterface replica = paxosInstances.get(getPaxosKeyFromPaxosID(paxosID));
      if (replica == null) return null;
      try {
        GNS.getLogger().info(" Proposing to  " + replica.getPaxosID());
        replica.handleIncomingMessage(requestPacket.toJSONObject(), PaxosPacketType.REQUEST);
      } catch (JSONException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
      return replica.getPaxosID();
    }

    //  only in debug mode
    try
    {
      JSONObject json = requestPacket.toJSONObject();
      // put paxos ID for identification
      json.put(PAXOS_ID, paxosID);
      handleIncomingPacket(json);

    } catch (JSONException e)
    {
      if (StartNameServer.debugMode) GNS.getLogger().severe(" JSON Exception" + e.getMessage());
      e.printStackTrace();
    }
    return paxosID;
  }

  /**
   * check if the failure detector has reported this node as up
   * @param nodeID  ID of node to be tested
   * @return <code>true</code> if failure detector tells node is up
   */
  public  boolean isNodeUp(int nodeID) {
    if (failureDetection == null) return true;
    return failureDetection.isNodeUp(nodeID);
  }

  /**
   * Returns -1 if failure detection not initialized, otherwise returns timeout for failure detection.
   * @return
   */
  public int getFailureDetectionTimeout() {
    if (failureDetection == null) return -1;
    return failureDetection.timeoutIntervalMillis;
  }

  /**
   * Handle incoming message, incoming message could be of any Paxos instance.
   * @param json json obejct received
   */
  public  void handleIncomingPacket(JSONObject json) throws JSONException {

    int incomingPacketType;
    try {
      incomingPacketType = json.getInt(PaxosPacketType.ptype);
    } catch (JSONException e) {
      e.printStackTrace();
      return;
    }
    switch (incomingPacketType){

      case PaxosPacketType.DECISION:
        try {
          paxosLogger.logMessage(new LoggingCommand(json.getString(PAXOS_ID), json, LoggingCommand.LOG_AND_EXECUTE));
        } catch (JSONException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        break;
      case PaxosPacketType.FAILURE_DETECT:
      case PaxosPacketType.FAILURE_RESPONSE:
        processMessage(new HandleFailureDetectionPacketTask(json, failureDetection));
        break;
      default:
        GNS.getLogger().fine("Received packet: " + json);
        processMessage(new HandlePaxosMessageTask(json,incomingPacketType, this));
        break;
    }
  }

  /**
   *
   * @param paxosID paxosID of the paxos group
   * @param req request that is to be executed
   */
  void handleDecision(String paxosID, RequestPacket req, boolean recovery) {
    clientRequestHandler.handlePaxosDecision(paxosID, req, recovery);
  }

  String getPaxosKeyFromPaxosID(String paxosID) {
    return clientRequestHandler.getPaxosKeyForPaxosID(paxosID);
  }

  /**
   * If a node fails, or comes up again, the respective Paxos instances are informed.
   * Some of them may elect a new co-ordinator.
   */
  void informNodeStatus(FailureDetectionPacket fdPacket) {
    GNS.getLogger().info("Handling node failure = " + fdPacket.responderNodeID);
    for (String x: paxosInstances.keySet()) {
      PaxosReplicaInterface r = paxosInstances.get(x);

      if (r.isNodeInPaxosInstance(fdPacket.responderNodeID)) {
        try
        {
          r.handleIncomingMessage(fdPacket.toJSONObject(), fdPacket.packetType);
        } catch (JSONException e)
        {
          if (StartNameServer.debugMode) GNS.getLogger().fine(" JSON Exception");
          e.printStackTrace();
        }
      }
    }
    // inform output handler of node failure
    clientRequestHandler.handleFailureMessage(fdPacket);
  }

  void sendMessage(int destID, JSONObject json, String paxosID) {
    try {
      json.put(PaxosManager.PAXOS_ID, paxosID);
      sendMessage(destID, json);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

  }

  void sendMessage(Set<Integer> destIDs, JSONObject json, String paxosID) {
    try {
      json.put(PaxosManager.PAXOS_ID, paxosID);
      sendMessage(destIDs, json);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

  }

  void sendMessage(short[] destIDs, JSONObject json, String paxosID, int excludeID) {
    try {
      json.put(PaxosManager.PAXOS_ID, paxosID);
      sendMessage(destIDs, json, excludeID);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

  }


  void addToActiveProposals(ProposalStateAtCoordinator propState) {
    synchronized (proposalStates) {
      proposalStates.add(propState);
    }
  }

  void removeFromActiveProposals(ProposalStateAtCoordinator propState) {
    synchronized (proposalStates){
      proposalStates.remove(propState);
    }
  }

  /********************END: public methods for paxos manager********************/


  /**
   * Once recovery is complete (@code recoveryComplete) = true, this method starts the following actions:
   * 1. garbage collection of paxos logs
   * 2. logging state for all paxos instances to disk periodically
   * 3. resend any lost messages for paxos replicas
   */
  private  void startPaxosMaintenanceActions() {
    startPaxosLogDeletionTask();
    startPaxosStateLogging();
    startResendPendingMessages();
  }

//  /**
//   * set the paxos log folder to given value
//   * @param paxosLogFolder name of the folder
//   */
//  public  void setPaxosLogFolder(String paxosLogFolder) {
//    PaxosLogger.setLoggerParameters(paxosLogFolder);
//  }

  /**
   * delete paxos logs that are useless
   */
  private  void startPaxosLogDeletionTask() {
    LogDeletionTask delTask = new LogDeletionTask(paxosLogger);
    executorService.scheduleAtFixedRate(delTask,(long)((0.5 + new Random().nextDouble())* PAXOS_LOG_DELETE_INTERVAL_SEC),
            PAXOS_LOG_DELETE_INTERVAL_SEC, TimeUnit.SECONDS);
  }

  /**
   *
   */
  private  void startPaxosStateLogging() {
    long delay = (long) ((0.5 + new Random().nextDouble()) * PAXOS_LOG_STATE_INTERVAL_SEC);
    LogPaxosStateTask logTask = new LogPaxosStateTask(this, paxosLogger);
    executorService.scheduleAtFixedRate(logTask, delay, PAXOS_LOG_STATE_INTERVAL_SEC, TimeUnit.SECONDS);
  }

  /**
   * set recovery complete to true if we have received synchronized list of Paxos instances
   * with all nodes that have not failed, and all nodes have responded.
   */
  private  void startAllPaxosReplicas() {
    if (paxosInstances!=null) {
      for (String x: paxosInstances.keySet()) {
        if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos Recovery: starting paxos replica .. " + x);
        paxosInstances.get(x).checkInitScout();
      }
    }
  }


  /**
   * Create a paxos instance for testing/debugging.
   */
  private  void createTestPaxosInstance(String testPaxosID) {
    if (paxosInstances.containsKey(getPaxosKeyFromPaxosID(testPaxosID))) {
      if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instance " + testPaxosID + " already exists.");
      return;
    }
    // create a default paxos instance for testing.
    Set<Integer> x = new HashSet<Integer>();
    for (int i = 0;  i < N; i++)
      x.add(i);
    createPaxosInstance(testPaxosID, x, clientRequestHandler.getState(testPaxosID));
  }

  /**
   * initialize transport object during Paxos debugging/testing
   *
   */
  private  NioServer initTransport(int numNodes, int startingPort) {

    // demux object for paxos
    PaxosPacketDemultiplexer paxosDemux = new PaxosPacketDemultiplexer(this);

    // node config object for test
    PaxosNodeConfig config = new PaxosNodeConfig(numNodes, startingPort);


    NioServer tcpTransportLocal = null;
    try {
      GNS.getLogger().fine(" Node ID is " + nodeID);

      ByteStreamToJSONObjects jsonMessageWorker = new ByteStreamToJSONObjects(paxosDemux);
      tcpTransportLocal = new NioServer(nodeID, jsonMessageWorker, config);
//      JSONMessageWorker jsonMessageWorker = new JSONMessageWorker(paxosDemux);
//      tcpTransportLocal = new NioServer(nodeID, config, jsonMessageWorker);


      if (StartNameServer.debugMode) GNS.getLogger().fine(" TRANSPORT OBJECT CREATED for node  " + nodeID);

      // start TCP transport thread
      new Thread(tcpTransportLocal).start();
    } catch (IOException e) {
      GNS.getLogger().severe(" Could not initialize TCP socket at client");
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    return tcpTransportLocal;
  }

  private  void processMessage(Runnable runnable) {
    if (executorService!= null) executorService.submit(runnable);
  }

  /**
   * all paxos instances use this method to exchange messages
   * @param destID send message to this node
   * @param json  json object to send
   */
  private  void sendMessage(int destID, JSONObject json) {
    try
    {
      if (!debug) {
        Packet.putPacketType(json, Packet.PacketType.PAXOS_PACKET);

      }
//      GNS.getLogger().fine("Sending message to " + destID + "\t" + json);
      nioServer.sendToID(destID, json);
    } catch (IOException e)
    {
      GNS.getLogger().severe("Paxos: IO Exception in sending to ID. " + destID);
    } catch (JSONException e)
    {
      GNS.getLogger().severe("JSON Exception in sending to ID. " + destID);
    }
  }

  private  void sendMessage(Set<Integer> destIDs, JSONObject json) {
    try {
      if (!debug) {
        Packet.putPacketType(json, Packet.PacketType.PAXOS_PACKET);
      }
      nioServer.sendToIDs(destIDs, json);
    } catch (IOException e)
    {
      GNS.getLogger().severe("Paxos: IO Exception in sending to IDs. " + destIDs);
    } catch (JSONException e)
    {
      GNS.getLogger().severe("JSON Exception in sending to IDs. " + destIDs);
    }
  }

  private  void sendMessage(short[] destIDs, JSONObject json, int excludeID) {
    try {
      if (!debug) {
        Packet.putPacketType(json, Packet.PacketType.PAXOS_PACKET);
      }
      nioServer.sendToIDs(destIDs, json, excludeID);
    } catch (IOException e)
    {
      GNS.getLogger().severe("Paxos: IO Exception in sending to IDs. ");
    } catch (JSONException e)
    {
      GNS.getLogger().severe("JSON Exception in sending to IDs. ");
    }
  }

  /**
   * Main function to test the paxos manager code.
   */
  public static void main(String[] args) {
//    if (args.length != 4) {
//      System.out.println("QUIT. Incorrect arguments.\nUsage: PaxosManager <NumberOfPaoxsNodes> <ReplicaID> <NodeConfigFile>");
//      System.exit(2);
//    }
    // node IDs (for paxos replicas) = 0, 1, ..., N - 1
    String nodeConfigFile = "resources/testCodeResources/nodeConfig"; //args[0];
    String testConfig = "resources/testCodeResources/testConfig";//args[1];
    int myID = 0; //Integer.parseInt(args[2]);

//    new PaxosManager(nodeConfigFile, testConfig, myID);

    // TODO fix these
//    PaxosManager.setPaxosLogFolder(paxosLogFolder + "/paxoslog_" + myID);
//    initializePaxosManagerDebugMode(nodeConfigFile, testConfig, myID, new DefaultPaxosInterface());
  }

}

/**
 * Packet demultiplexer object
 */
class PaxosPacketDemultiplexer extends PacketDemultiplexer {

  PaxosManager paxosManager;

  public PaxosPacketDemultiplexer(PaxosManager paxosManager) {
    this.paxosManager = paxosManager;
  }
  @Override
  public void handleJSONObjects(ArrayList jsonObjects) {
    for (Object j: jsonObjects) {
      JSONObject json = (JSONObject)j;
      try {
        paxosManager.handleIncomingPacket(json);
      } catch (JSONException e) {
        e.printStackTrace();
      }
    }
  }
}


class HandlePaxosMessageTask extends TimerTask {

  JSONObject json;

  int packetType;

  PaxosManager paxosManager;

  HandlePaxosMessageTask(JSONObject json, int packetType, PaxosManager paxosManager){
    this.json = json;
    this.packetType = packetType;
    this.paxosManager = paxosManager;
  }

  @Override
  public void run() {

    long t0 = System.currentTimeMillis();
    try {
      String paxosID;
      try {
        paxosID = json.getString(PaxosManager.PAXOS_ID);
      } catch (JSONException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        return;
      }

      PaxosReplicaInterface replica = paxosManager.paxosInstances.get(paxosManager.getPaxosKeyFromPaxosID(paxosID));
      if (replica != null && replica.getPaxosID().equals(paxosID)) {
        replica.handleIncomingMessage(json,packetType);
      }
      else {
        // this case can arise just after a paxos instance is created or stopped.
        GNS.getLogger().warning("ERROR: Paxos Instances does not contain ID = " + paxosID + " key set: " + paxosManager.paxosInstances.keySet());
      }
    } catch (Exception e) {
      GNS.getLogger().severe(" PAXOS Exception EXCEPTION!!. Msg = " + json);
      e.printStackTrace();
    }
    long t1 = System.currentTimeMillis();
    if (t1 - t0 > 100)
      GNS.getLogger().severe("Long delay " + (t1 - t0) + "ms. Packet: " + json);
  }
}

/**
 * Resend proposals (for all paxos instances) that have not yet been accepted by majority.
 */
class ResendPendingMessagesTask extends TimerTask{

  PaxosManager paxosManager;

  public ResendPendingMessagesTask(PaxosManager paxosManager) {
    this.paxosManager = paxosManager;
  }

  @Override
  public void run() {

    ArrayList<ProposalStateAtCoordinator> reattempts = new ArrayList<ProposalStateAtCoordinator>();

    try{
      // synchronization over
      synchronized (paxosManager.proposalStates){
        for (ProposalStateAtCoordinator propState: paxosManager.proposalStates) {
          if (propState.getTimeSinceAccept() > PaxosManager.RESEND_PENDING_MSG_INTERVAL_MILLIS) {
            reattempts.add(propState);
          }
          else break;
        }
      }
      for (ProposalStateAtCoordinator propState: reattempts) {
        boolean result = propState.paxosReplica.resendPendingProposal(propState);
        if (!result) {
          synchronized (paxosManager.proposalStates){
            paxosManager.proposalStates.remove(propState);
          }
        }
        if (StartNameServer.experimentMode) GNS.getLogger().severe("\tResendingMessage\t" +
                propState.paxosReplica.getPaxosID() + "\t" + propState.pValuePacket.proposal.slot + "\t" + result + "\t");
      }
      paxosManager.startResendPendingMessages();

    }catch (Exception e) {
      GNS.getLogger().severe("Exception in sending pending messages." + e.getMessage());
      e.printStackTrace();
    }

  }
}
