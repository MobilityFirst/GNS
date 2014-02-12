package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaController;
import edu.umass.cs.gns.nio.ByteStreamToJSONObjects;
import edu.umass.cs.gns.nio.NioServer;
import edu.umass.cs.gns.nio.PacketDemultiplexer;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.paxospacket.FailureDetectionPacket;
import edu.umass.cs.gns.packet.paxospacket.PaxosPacketType;
import edu.umass.cs.gns.packet.paxospacket.RequestPacket;
import edu.umass.cs.gns.packet.paxospacket.StatePacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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
public class  PaxosManager extends Thread{

  static final String PAXOS_ID = "PXS";

  /**
   *  total number of nodes. node IDs = 0, 1, ..., N -1
   */
  private static int N;

  /**
   * nodeID of this node. among node IDs = 0, 1, ..., (N - 1)
   */
  static int nodeID;

  /**
   * When paxos is run independently {@code tcpTransport} is used to send messages between paxos replicas and client.
   */
  private static NioServer tcpTransport;

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
  static ConcurrentHashMap<String, PaxosReplicaInterface> paxosInstances = new ConcurrentHashMap<String, PaxosReplicaInterface>();

  static PaxosInterface clientRequestHandler;

  static ScheduledThreadPoolExecutor executorService;

  static final TreeSet<ProposalStateAtCoordinator> proposalStates = new TreeSet<ProposalStateAtCoordinator>();

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


  private static int maxThreads = 5;

  /**
   * debug = true is used to debug the paxos module,  debug = false when complete GNRS system is running.
   */
  private static boolean debug = false;

  /**
   * Paxos ID of the paxos instance created for testing/debugging.
   */
  static final String defaultPaxosID  = "0-P";

  /********************BEGIN: public methods for paxos manager********************/

  public static void initializePaxosManager(int numberOfNodes, int nodeID, NioServer tcpTransport,
                                          PaxosInterface outputHandler, ScheduledThreadPoolExecutor executorService) {

    PaxosManager.N = numberOfNodes;
    PaxosManager.nodeID = nodeID;
    PaxosManager.tcpTransport = tcpTransport;

    PaxosManager.clientRequestHandler = outputHandler;

    PaxosManager.executorService = executorService;

    FailureDetection.initializeFailureDetection(N, nodeID);

    // recover previous state if exists using logger

    ConcurrentHashMap<String, PaxosReplicaInterface> myPaxosInstances = PaxosLogger.initLogger(nodeID);
    if (myPaxosInstances != null) paxosInstances = myPaxosInstances;

    if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instances: " + paxosInstances.size());

    if (debug && paxosInstances.size() == 0) createDefaultPaxosInstance();
    else startAllPaxosReplicas();

    startPaxosMaintenanceActions();

    GNS.getLogger().info("Paxos manager initialization complete");

  }

  public static void initializePaxosManagerDebugMode(String nodeConfig, String testConfig, int nodeID, PaxosInterface outputHandler) {
    // set debug mode to true
    debug = true;
    // read node configs

    readTestConfigFile(testConfig);

    PaxosManager.nodeID = nodeID;

    // init paxos manager
    initializePaxosManager(N, nodeID, initTransport(nodeConfig), outputHandler, new ScheduledThreadPoolExecutor(maxThreads));

  }

  /**
   * Adds a new Paxos instance to the set of actives.
   */
  public static boolean createPaxosInstance(String paxosID, Set<Integer> nodeIDs, String initialState) {

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
      r1 = paxosInstances.get(PaxosManager.getPaxosKeyFromPaxosID(paxosID));

      if (r1 != null && r1.getPaxosID().equals(paxosID)) {
        GNS.getLogger().warning("Paxos replica already exists .. " + paxosID);
        return false;

      } else {

        assert initialState != null;
        r = createPaxosReplicaObject(paxosID, nodeID, nodeIDs);//new PaxosReplicaInterface(paxosID, nodeID, nodeIDs);
        if (!StartNameServer.experimentMode) { // During experiments, we disable state logging. This helps us load records faster into database.
          PaxosLogger.logPaxosStart(paxosID, nodeIDs, new StatePacket(r.getAcceptorBallot(), 0, initialState));
        }
        if(StartNameServer.debugMode) GNS.getLogger().info(paxosID + "\tBefore creating replica.");
        paxosInstances.put(PaxosManager.getPaxosKeyFromPaxosID(paxosID), r);
      }
    }

    if (r1 != null && !r1.getPaxosID().equals(paxosID)) {
      r1.removePendingProposals();
      GNS.getLogger().info("OldPaxos replica replaced .. so log a stop message .. " + r1.getPaxosID() + " new replica " + paxosID);
      PaxosLogger.logPaxosStop(r1.getPaxosID());    // multiple stop msgs can get logged because other replica might stop in meanwhile.
    }

    if (r!= null) {
      r.checkCoordinatorFailure(); // propose new ballot if default coordinator has failed
    }
    return true;
  }

  public static void resetAll() {
    // delete paxos instances
    paxosInstances.clear();
    // clear paxos logs
    PaxosLogger.clearLogs();
    // run java gc
  }

  /**
   *
   */
  static void startResendPendingMessages() {
    ResendPendingMessagesTask task = new ResendPendingMessagesTask();
    // single time execution
    executorService.schedule(task, RESEND_PENDING_MSG_INTERVAL_MILLIS,TimeUnit.MILLISECONDS);
  }

  /**
   * Set the failure detection ping interval to <code>intervalMillis</code>
   */
  public static void setFailureDetectionPingInterval(int intervalMillis) {
    FailureDetection.pingIntervalMillis = intervalMillis;
  }

  /**
   * Set the failure detection timeout to <code>intervalMillis</code>
   */
  public static void setFailureDetectionTimeoutInterval(int intervalMillis) {
    FailureDetection.timeoutIntervalMillis = intervalMillis;
  }

  static PaxosReplicaInterface createPaxosReplicaObject(String paxosID, int nodeID, Set<Integer> nodeIDs1) {
    if (StartNameServer.experimentMode){
      if (ReplicaController.isPrimaryPaxosID(paxosID)) { // Paxos among primaries uses normal paxos instances.
        return new PaxosReplica(paxosID, nodeID, nodeIDs1);
      } else {
        return new PaxosReplicaNew(paxosID, nodeID, nodeIDs1);
      }
    } else {
      return new PaxosReplica(paxosID, nodeID, nodeIDs1);
    }
  }

  /**
   * Propose requestPacket in the paxos instance with paxosID.
   * ReqeustPacket.clientID is used to distinguish which method proposed this value.
   * @param paxosID paxosID of the paxos group
   * @param requestPacket request to be proposed
   */
  public static String propose(String paxosID, RequestPacket requestPacket) {

    if (!debug) { // running with GNS
      PaxosReplicaInterface replica = paxosInstances.get(PaxosManager.getPaxosKeyFromPaxosID(paxosID));
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
  public static boolean isNodeUp(int nodeID) {
    return FailureDetection.isNodeUp(nodeID);
  }

  /**
   * Handle incoming message, incoming message could be of any Paxos instance.
   * @param json json obejct received
   */
  public static void handleIncomingPacket(JSONObject json) throws JSONException {

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
          PaxosLogger.logMessage(new LoggingCommand(json.getString(PAXOS_ID), json, LoggingCommand.LOG_AND_EXECUTE));
        } catch (JSONException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        break;
      case PaxosPacketType.FAILURE_DETECT:
      case PaxosPacketType.FAILURE_RESPONSE:
        processMessage(new HandleFailureDetectionPacketTask(json));
        break;
      default:
        processMessage(new HandlePaxosMessageTask(json,incomingPacketType));
    }
  }

  /**
   *
   * @param paxosID paxosID of the paxos group
   * @param req request that is to be executed
   */
  static void handleDecision(String paxosID, RequestPacket req, boolean recovery) {
    clientRequestHandler.handlePaxosDecision(paxosID, req, recovery);
  }

  static String getPaxosKeyFromPaxosID(String paxosID) {
    if (debug) return paxosID;
    if (ReplicaController.isPrimaryPaxosID(paxosID)) return paxosID; // paxos between primaries
    else { // paxos between actives.
      int index = paxosID.lastIndexOf("-");
      if (index == -1) return paxosID;
      return paxosID.substring(0, index);
    }
  }

  /**
   * If a node fails, or comes up again, the respective Paxos instances are informed.
   * Some of them may elect a new co-ordinator.
   */
  static void informNodeStatus(FailureDetectionPacket fdPacket) {
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

  static void sendMessage(int destID, JSONObject json, String paxosID) {
    try {
      json.put(PaxosManager.PAXOS_ID, paxosID);
      sendMessage(destID, json);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

  }

  static void sendMessage(Set<Integer> destIDs, JSONObject json, String paxosID) {
    try {
      json.put(PaxosManager.PAXOS_ID, paxosID);
      sendMessage(destIDs, json);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

  }

  static void sendMessage(short[] destIDs, JSONObject json, String paxosID, int excludeID) {
    try {
      json.put(PaxosManager.PAXOS_ID, paxosID);
      sendMessage(destIDs, json, excludeID);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

  }


  static void addToActiveProposals(ProposalStateAtCoordinator propState) {
    synchronized (proposalStates) {
      proposalStates.add(propState);
    }
  }

  static void removeFromActiveProposals(ProposalStateAtCoordinator propState) {
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
  private static void startPaxosMaintenanceActions() {
    startPaxosLogDeletionTask();
    startPaxosStateLogging();
    startResendPendingMessages();
  }

  /**
   * set the paxos log folder to given value
   * @param paxosLogFolder name of the folder
   */
  public static void setPaxosLogFolder(String paxosLogFolder) {
    PaxosLogger.setLoggerParameters(paxosLogFolder);
  }

  /**
   * delete paxos logs that are useless
   */
  private static void startPaxosLogDeletionTask() {
    LogDeletionTask delTask = new LogDeletionTask();
    executorService.scheduleAtFixedRate(delTask,(long)(0.5 + new Random().nextDouble())* PAXOS_LOG_DELETE_INTERVAL_SEC,
            PAXOS_LOG_DELETE_INTERVAL_SEC, TimeUnit.SECONDS);
  }

  /**
   *
   */
  private static void startPaxosStateLogging() {
    LogPaxosStateTask logTask = new LogPaxosStateTask();
    executorService.scheduleAtFixedRate(logTask, (long) (0.5 + new Random().nextDouble()) * PAXOS_LOG_STATE_INTERVAL_SEC,
            PAXOS_LOG_STATE_INTERVAL_SEC, TimeUnit.SECONDS);
  }

  /**
   * set recovery complete to true if we have received synchronized list of Paxos instances
   * with all nodes that have not failed, and all nodes have responded.
   */
  private static void startAllPaxosReplicas() {
    if (paxosInstances!=null) {
      for (String x: PaxosManager.paxosInstances.keySet()) {
        if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos Recovery: starting paxos replica .. " + x);
        PaxosManager.paxosInstances.get(x).checkInitScout();
      }
    }
  }

  /**
   * read config file during testing/debugging
   * @param testConfig configuration file used during testing
   */
  private static void readTestConfigFile(String testConfig) {
    File f = new File(testConfig);
    if (!f.exists()) {
      if (StartNameServer.debugMode) GNS.getLogger().fine(" testConfig file does not exist. Quit. " +
              "Filename =  " + testConfig);
      System.exit(2);
    }

    try {
      BufferedReader br = new BufferedReader(new FileReader(f));
      while (true) {
        String line = br.readLine();
        if (line == null) break;
        String[] tokens = line.trim().split("\\s+");
        if (tokens.length != 2) continue;
        if (tokens[0].equals("NumberOfReplicas")) N = Integer.parseInt(tokens[1]);
        else if (tokens[0].equals("EnableLogging")) StartNameServer.debugMode = Boolean.parseBoolean(tokens[1]);
        else if (tokens[0].equals("MaxThreads")) maxThreads = Integer.parseInt(tokens[1]);
        else if (tokens[0].equals("GarbageCollectionInterval")) {
          MEMORY_GARBAGE_COLLECTION_INTERVAL = Integer.parseInt(tokens[1]);
        }

      }
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  /**
   * Create a paxos instance for testing/debugging.
   */
  private static void createDefaultPaxosInstance() {
    if (paxosInstances.containsKey(PaxosManager.getPaxosKeyFromPaxosID(defaultPaxosID))) {
      if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instance " + defaultPaxosID + " already exists.");
      return;
    }
    // create a default paxos instance for testing.
    Set<Integer> x = new HashSet<Integer>();
    for (int i = 0;  i < N; i++)
      x.add(i);
    createPaxosInstance(defaultPaxosID, x, clientRequestHandler.getState(defaultPaxosID));
  }

  /**
   * initialize transport object during Paxos debugging/testing
   * @param configFile config file containing list of node ID, IP, port
   */
  private static NioServer initTransport(String configFile) {

    // create the worker object
    PaxosPacketDemultiplexer paxosDemux = new PaxosPacketDemultiplexer();
    ByteStreamToJSONObjects worker = new ByteStreamToJSONObjects(paxosDemux);

    // start TCP transport thread
    NioServer tcpTransportLocal = null;
    try {
      GNS.getLogger().fine(" Node ID is " + nodeID);
      tcpTransportLocal = new NioServer(nodeID, worker, new PaxosNodeConfig(configFile));
      if (StartNameServer.debugMode) GNS.getLogger().fine(" TRANSPORT OBJECT CREATED for node  " + nodeID);
      new Thread(tcpTransportLocal).start();
    } catch (IOException e) {
      GNS.getLogger().severe(" Could not initialize TCP socket at client");
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    return tcpTransportLocal;
  }

  private static void processMessage(Runnable runnable) {
    if (executorService!= null) executorService.submit(runnable);
  }

  /**
   * all paxos instances use this method to exchange messages
   * @param destID send message to this node
   * @param json  json object to send
   */
  private static void sendMessage(int destID, JSONObject json) {
    try
    {
      if (!debug) {
        Packet.putPacketType(json, Packet.PacketType.PAXOS_PACKET);

      }
//      GNS.getLogger().fine("Sending message to " + destID + "\t" + json);
      tcpTransport.sendToID(destID, json);
    } catch (IOException e)
    {
      GNS.getLogger().severe("Paxos: IO Exception in sending to ID. " + destID);
    } catch (JSONException e)
    {
      GNS.getLogger().severe("JSON Exception in sending to ID. " + destID);
    }
  }

  private static void sendMessage(Set<Integer> destIDs, JSONObject json) {
    try {
      if (!debug) {
        Packet.putPacketType(json, Packet.PacketType.PAXOS_PACKET);
      }
      tcpTransport.sendToIDs(destIDs, json);
    } catch (IOException e)
    {
      GNS.getLogger().severe("Paxos: IO Exception in sending to IDs. " + destIDs);
    } catch (JSONException e)
    {
      GNS.getLogger().severe("JSON Exception in sending to IDs. " + destIDs);
    }
  }

  private static void sendMessage(short[] destIDs, JSONObject json, int excludeID) {
    try {
      if (!debug) {
        Packet.putPacketType(json, Packet.PacketType.PAXOS_PACKET);
      }
      tcpTransport.sendToIDs(destIDs, json, excludeID);
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
    if (args.length != 4) {
      System.out.println("QUIT. Incorrect arguments.\nUsage: PaxosManager <NumberOfPaoxsNodes> <ReplicaID> <NodeConfigFile>");
      System.exit(2);
    }
    // node IDs (for paxos replicas) = 0, 1, ..., N - 1
    String nodeConfigFile = args[0];
    String testConfig = args[1];
    String paxosLogFolder = args[2];
    int myID = Integer.parseInt(args[3]);
    PaxosManager.setPaxosLogFolder(paxosLogFolder + "/paxoslog_" + myID);
    initializePaxosManagerDebugMode(nodeConfigFile, testConfig, myID, new DefaultPaxosInterface());
  }

}

class PaxosPacketDemultiplexer extends PacketDemultiplexer {

  @Override
  public void handleJSONObjects(ArrayList jsonObjects) {
    for (Object j: jsonObjects) {
      JSONObject json = (JSONObject)j;
      try {
        PaxosManager.handleIncomingPacket(json);
      } catch (JSONException e) {
        e.printStackTrace();
      }
    }
  }
}


class HandlePaxosMessageTask extends TimerTask {

  JSONObject json;

  int packetType;


  HandlePaxosMessageTask(JSONObject json, int packetType){
    this.json = json;
    this.packetType = packetType;
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

      PaxosReplicaInterface replica = PaxosManager.paxosInstances.get(PaxosManager.getPaxosKeyFromPaxosID(paxosID));
      if (replica != null && replica.getPaxosID().equals(paxosID)) {
        replica.handleIncomingMessage(json,packetType);
      }
      else {
        // this case can arise just after a paxos instance is created or stopped.
        GNS.getLogger().warning("ERROR: Paxos Instances does not contain ID = " + paxosID);
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

  @Override
  public void run() {

    ArrayList<ProposalStateAtCoordinator> reattempts = new ArrayList<ProposalStateAtCoordinator>();

    try{
      // synchronization over
      synchronized (PaxosManager.proposalStates){
        for (ProposalStateAtCoordinator propState: PaxosManager.proposalStates) {
          if (propState.getTimeSinceAccept() > PaxosManager.RESEND_PENDING_MSG_INTERVAL_MILLIS) {
            reattempts.add(propState);
          }
          else break;
        }
      }
      for (ProposalStateAtCoordinator propState: reattempts) {
        boolean result = propState.paxosReplica.resendPendingProposal(propState);
        if (!result) {
          synchronized (PaxosManager.proposalStates){
            PaxosManager.proposalStates.remove(propState);
          }
        }
        if (StartNameServer.experimentMode) GNS.getLogger().severe("\tResendingMessage\t" +
                propState.paxosReplica.getPaxosID() + "\t" + propState.pValuePacket.proposal.slot + "\t" + result + "\t");
      }
      PaxosManager.startResendPendingMessages();

    }catch (Exception e) {
      GNS.getLogger().severe("Exception in sending pending messages." + e.getMessage());
      e.printStackTrace();
    }

  }
}


/**
 * periodically logs state of all paxos instances
 */
class LogPaxosStateTask extends TimerTask {

  @Override
  public void run() {
    try {

      if (StartNameServer.experimentMode) {return;} // we do not log paxos state during experiments ..

      GNS.getLogger().info("Logging paxos state task.");
      for (String paxosKey: PaxosManager.paxosInstances.keySet()) {

        PaxosReplicaInterface replica = PaxosManager.paxosInstances.get(paxosKey);
        if (paxosKey != null) {
          StatePacket packet = replica.getState();
          if (packet != null) {
            PaxosLogger.logPaxosState(replica.getPaxosID(), packet);
          }
        }
      }
      GNS.getLogger().info("Completed logging.");
    }catch(Exception e) {
      GNS.getLogger().severe("Exception IN paxos state logging " + e.getMessage());
      e.printStackTrace();
    }
  }

}
