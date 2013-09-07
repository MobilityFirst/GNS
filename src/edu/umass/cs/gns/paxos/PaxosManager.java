package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nio.ByteStreamToJSONObjects;
import edu.umass.cs.gns.nio.NioServer2;
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
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages a set of Paxos instances, each instance could be running 
 * on a different susbset of nodes but including this node.  
 * @author abhigyan
 *
 */
public class PaxosManager extends Thread{

  static final String PAXOS_ID = "PXS";

  private final static String REQUEST_ID = "RQID";

//    public static final int timeoutForRequestState = 1000;

  /**
   *  total number of nodes. node IDs = 0, 1, ..., N -1
   */
  static int N;

  /**
   * nodeID of this node. among node IDs = 0, 1, ..., (N - 1)
   */
  static int nodeID;

  /**
   * When paxos is run independently {@code tcpTransport} is used to send messages between paxos replicas and client.
   */
  static NioServer2 tcpTransport;

  /**
   * a hash table of where K = paxos ID, V = Paxos replica
   */
  static ConcurrentHashMap<String, PaxosReplica> paxosInstances = new ConcurrentHashMap<String, PaxosReplica>();


  static PaxosInterface clientRequestHandler;

  static ScheduledThreadPoolExecutor executorService;

  static int maxThreads = 5;

  /**
   * debug = true is used to debug the paxos module,  debug = false when complete GNRS system is running.
   */
  static boolean debug = false;
  /**
   * Paxos ID of the paxos instance created for testing/debugging.
   */
  static String defaultPaxosID  = "0";

  /**
   * Minimum interval (in milliseconds) between two garbage state collections of replicas.
   */
  static int MEMORY_GARBAGE_COLLECTION_INTERVAL = 100;

  /**
   * Paxos coordinator checks whether all replicas have received the latest messages decided.
   */
  private static long RESEND_PENDING_MSG_INTERVAL_SEC = 1;

  /**
   * Paxos logs are garbage collected at this interval
   */
  private static long PAXOS_LOG_STATE_INTERVAL_SEC = 3600;

  /**
   * Redundant paxos logs are checked and deleted at this interval.
   */
  private static long PAXOS_LOG_DELETE_INTERVAL_SEC = 3600;



  /**
   * variable is true if paxos recovery is complete
   */
  private static boolean recoveryComplete = false;

  /**
   * object used to synchronize access to {@code recoveryComplete}
   */
  private static Object recoveryCompleteLock = new ReentrantLock();

  /**
   * @return true if paxos has completed recovery, false otherwise
   */
  public static boolean isRecoveryComplete() {
    synchronized (recoveryCompleteLock) {
      return recoveryComplete;
    }
  }

  /**
   * method used to change the status of {@code recoveryComplete} object
   * @param status
   */
  private static void setRecoveryComplete(boolean status) {
    synchronized (recoveryCompleteLock) {
      recoveryComplete = status;
    }
  }

  public static void initializePaxosManager(int numberOfNodes, int nodeID, NioServer2 tcpTransport, PaxosInterface outputHandler, ScheduledThreadPoolExecutor executorService) {

    PaxosManager.N = numberOfNodes;
    PaxosManager.nodeID = nodeID;
    PaxosManager.tcpTransport = tcpTransport;

    PaxosManager.clientRequestHandler = outputHandler;

    PaxosManager.executorService = executorService;

    FailureDetection.initializeFailureDetection(N, nodeID);

    // recover previous state if exists using logger
//        ConcurrentHashMap<String, PaxosReplica> paxosInstances = PaxosLogger.initializePaxosLogger();
    // step 1: do local log recovery
    paxosInstances = PaxosLogger2.initLogger();
//      System.exit(2);
    if (paxosInstances == null) {
      paxosInstances = new ConcurrentHashMap<String, PaxosReplica>();

    }
    if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instances: " + paxosInstances.size());

//      if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instances slot : " + paxosInstances.get(defaultPaxosID).getSlotNumber());
//      if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instances ballot: " + paxosInstances.get(defaultPaxosID).getAcceptorBallot());

    if (debug && paxosInstances.size() == 0) createDefaultPaxosInstance();
    else startAllPaxosReplicas();

    // step 2: start global log synchronization: what do we
    //
//      startLogSynchronization();

    // step 3: start check whether global log sync is over
    // TODO finish step2 and step 3

    startPaxosMaintenanceActions();


    if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos manager initialization complete");

  }

  public static void initializePaxosManagerDebugMode(String nodeConfig, String testConfig, int nodeID, PaxosInterface outputHandler) {
    // set debug mode to true
    debug = true;
    // read node configs

    readTestConfigFile(testConfig);

    PaxosManager.nodeID = nodeID;

    // init paxos manager
    initializePaxosManager(N, nodeID, initTransport(nodeConfig), outputHandler, new ScheduledThreadPoolExecutor(maxThreads));
//
//
////        ConcurrentHashMap<String, PaxosReplica> paxosInstances = PaxosLogger.initializePaxosLogger();
////        PaxosLogger2.initLogger();
//
//    // initialize executor service
//
//
//    if (paxosInstances!=null) {
//      PaxosManager.paxosInstances = paxosInstances;
//      for (String x: PaxosManager.paxosInstances.keySet()) {
//        if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos Recovery: starting paxos replica .. " + x);
//        PaxosManager.paxosInstances.get(x).startReplica();
//      }
//    }
//
//    FailureDetection.initializeFailureDetection(N, nodeID);
//    // create a paxos instance for debugging
//    createDefaultPaxosInstance();

//        ResendPendingMessagesTask task = new ResendPendingMessagesTask();
//        executorService.scheduleAtFixedRate(task,1000,1000, TimeUnit.MILLISECONDS);
  }


  /**
   * set the failure detection timeout to given value
   * @param interval
   */
  public static void setFailureDetectionPingInterval(int interval) {
    FailureDetection.pingInterval = interval;
  }

  /**
   * set the failure detection timeout to given value
   * @param interval
   */
  public static void setFailureDetectionTimeoutInterval(int interval) {
    FailureDetection.timeoutInterval = interval;

  }

  /**
   * set the paxos log folder to given value
   * @param paxosLogFolder
   */
  public static void setPaxosLogFolder(String paxosLogFolder) {
    PaxosLogger2.logFolder = paxosLogFolder;
  }



  /**
   *
   */
  private static ConcurrentHashMap<Integer, Integer> globalSyncNodesResponded;

  private static ConcurrentHashMap<String, String> newPaxosInstancesUninitialized;


  /**
   * On startup, this method start to synchronize logs of this paxos instance with other paxos instances
   */
  private static void startLogSynchronization() {
    globalSyncNodesResponded = new ConcurrentHashMap<Integer, Integer>();
    newPaxosInstancesUninitialized = new ConcurrentHashMap<String, String>();
    // TODO implement this method
//    sendPaxosInstanceListToOtherNodes();
  }

  /**
   * set recovery complete to true if we have received synchronized list of paxos instances
   * with all nodes that have not failed, and all nodes have responded.
   */
  static void checkLogSynchronizationOver() {
    // TODO write this method
    startPaxosMaintenanceActions();
  }


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
    executorService.scheduleAtFixedRate(logTask,(long)(0.5 + new Random().nextDouble())*PAXOS_LOG_STATE_INTERVAL_SEC,
            PAXOS_LOG_STATE_INTERVAL_SEC, TimeUnit.SECONDS);
  }

  /**
   *
   */
  private static void startResendPendingMessages() {
    ResendPendingMessagesTask task = new ResendPendingMessagesTask();
    executorService.scheduleAtFixedRate(task, 0,
            RESEND_PENDING_MSG_INTERVAL_SEC, TimeUnit.SECONDS);
  }

  /**
   * set recovery complete to true if we have received synchronized list of Paxos instances
   * with all nodes that have not failed, and all nodes have responded.
   */
  private static void startAllPaxosReplicas() {
    if (paxosInstances!=null) {
      for (String x: PaxosManager.paxosInstances.keySet()) {
        if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos Recovery: starting paxos replica .. " + x);
        PaxosManager.paxosInstances.get(x).startReplica();
      }
    }

  }

  /**
   *
   */
  private static void sendPaxosInstanceListToOtherNodes(ConcurrentHashMap<String, PaxosReplica> paxosInstances) {
    for (int i = 0; i < N; i++) {
      Set<String> paxosIDsActive = new HashSet<String>();
      Set<String> paxosIDsDeleted = new HashSet<String>();

      for (String x: paxosInstances.keySet()) {
        if (!paxosInstances.get(x).getNodeIDs().contains(i)) continue;
        if (paxosInstances.get(x).isStopped()) paxosIDsDeleted.add(x);
        else paxosIDsActive.add(x);
      }
      // send paxos IDs to node i
    }

  }

  private static void prepareListOfPaxosInstances(int senderNodeID, Set<String> paxosIDsActiveAtSendingNode,
                                                  Set<String> paxosIDsStoppedAtSendingNode) {
    synchronized (paxosInstances) {
      HashMap<String,Set<Integer>> paxosInstancesAdded = new HashMap<String, Set<Integer>>();
      HashMap<String,Set<Integer>> paxosInstancesStopped = new HashMap<String,Set<Integer>>();
      HashMap<String,RequestPacket> stopRequests = new HashMap<String,RequestPacket>();

      for (String paxosID: paxosInstances.keySet()) {
        //
        if (!paxosInstances.get(paxosID).isNodeInPaxosInstance(senderNodeID)) continue;

        PaxosReplica paxosReplica = paxosInstances.get(paxosID);
        if (paxosReplica.isStopped()) {
          if (!paxosIDsStoppedAtSendingNode.contains(paxosID)) {
            paxosInstancesStopped.put(paxosID, paxosReplica.getNodeIDs());
            stopRequests.put(paxosID,paxosReplica.getLastRequest());
          }
        }
        else {
          if (!paxosIDsActiveAtSendingNode.contains(paxosID))
            paxosInstancesAdded.put(paxosID,paxosReplica.getNodeIDs());
        }

      }
    }

  }

  /**
   * Handle newPaxosInstancesAdded(ConcurrentHashMap<String, P)
   * @param paxosInstancesAdded
   */
  private static void handlePaxosInstanceSetAdded(ConcurrentHashMap<String, Set<Integer>> paxosInstancesAdded) {
    synchronized (paxosInstances) {
      for (String paxosID: paxosInstancesAdded.keySet()) {
        if (paxosInstances.containsKey(paxosID)) {
          if (paxosInstances.get(paxosID).isStopped()) {
            // paxos ID is stopped, continue
            continue;
          }
          else {
            paxosInstances.get(paxosID).startReplica();
          }
        }
        else {
          PaxosReplica paxosReplica = new PaxosReplica(paxosID,nodeID,paxosInstancesAdded.get(nodeID));
          paxosInstances.put(paxosID,paxosReplica);
          // TODO get initial paxos state
          String initialPaxosState = "";
          PaxosLogger2.logPaxosStart(paxosID, paxosInstancesAdded.get(nodeID), null);
          if(StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tStarting replica");
          paxosReplica.startReplica();
        }
      }
    }
  }

  /**
   * Handle deletion of these paxos instances
   * @param paxosInstancesStopped
   */
  private static void handlePaxosInstanceSetStopped(ConcurrentHashMap<String, Set<Integer>> paxosInstancesStopped,
                                                    ConcurrentHashMap<String, RequestPacket> paxosInstaceStopRequests) {
    synchronized (paxosInstances) {
      for (String paxosID: paxosInstancesStopped.keySet()) {
        // paxos ID is already stopped, continue
        if (paxosInstances.containsKey(paxosID) && paxosInstances.get(paxosID).isStopped())
          continue;
        PaxosReplica paxosReplica = new PaxosReplica(paxosID,nodeID,paxosInstancesStopped.get(paxosID),true, paxosInstaceStopRequests.get(paxosID));
        paxosInstances.put(paxosID, paxosReplica);
        PaxosLogger2.logPaxosStop(paxosID);
//        PaxosLogger2.logPaxosStop(paxosID, paxosInstaceStopRequests.get(paxosID));
        clientRequestHandler.handlePaxosDecision(paxosID, paxosInstaceStopRequests.get(paxosID));
      }
    }
  }



  /**
   * read config file during testing/debugging
   * @param testConfig
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
    if (paxosInstances.containsKey(defaultPaxosID)) {
      if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instance " + defaultPaxosID + " already exists.");
      return;
    }
    // create a default paxos instance for testing.
    Set<Integer> x = new HashSet<Integer>();
    for (int i = 0;  i < N; i++)
      x.add(i);
    createPaxosInstance(defaultPaxosID, x, clientRequestHandler.getState(defaultPaxosID));


  }

//    /**
//     * initialize executor service during Paxos debugging/testing
//     */
//    private static void initExecutor() {
//
//        executorService = ;
//    }

  /**
   * initialize transport object during Paxos debugging/testing
   * @param configFile config file containing list of node ID, IP, port
   */
  private static NioServer2 initTransport(String configFile) {

    // create the worker object
    PaxosPacketDemultiplexer paxosDemux = new PaxosPacketDemultiplexer();
    ByteStreamToJSONObjects worker = new ByteStreamToJSONObjects(paxosDemux);

    // earlier worker was running as a separate thread
//        new Thread(worker).start();

    // start TCP transport thread
    NioServer2 tcpTransportLocal = null;
    try {
      GNS.getLogger().fine(" Node ID is " + nodeID);
      tcpTransportLocal = new NioServer2(nodeID, worker, new PaxosNodeConfig(configFile));
      if (StartNameServer.debugMode) GNS.getLogger().fine(" TRANSPORT OBJECT CREATED for node  " + nodeID);
      new Thread(tcpTransportLocal).start();
    } catch (IOException e) {
      GNS.getLogger().severe(" Could not initialize TCP socket at client");
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    return tcpTransportLocal;
  }

  public static void resetAll() {
    // delete paxos instances
    paxosInstances.clear();
    // clear paxos logs
    PaxosLogger2.clearLogs();
    // run java gc
    System.gc();
  }

  /**
   * Adds a new Paxos instance to the set of actives.
   */
  public static boolean createPaxosInstance(String paxosID, Set<Integer> nodeIDs, String initialState) {

    if(StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tEnter createPaxos");
    if (nodeIDs.size() < 3) {
      if (StartNameServer.debugMode) GNS.getLogger().severe(nodeID + " less than three replicas " +
              "paxos instance cannot be created. SEVERE ERROR. EXCEPTION.");
      return false;
    }

    if (!nodeIDs.contains(nodeID)) {
      if (StartNameServer.debugMode) GNS.getLogger().severe(nodeID + " this node not a member of paxos instance. replica not created.");
      return false;
    }

    PaxosReplica r;

    synchronized (paxosInstances)
    {

      if (paxosInstances.containsKey(paxosID)) {
        if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instance already exists. Paxos ID = " + paxosID);
        return false;
      }

      if (initialState != null) {
        PaxosLogger2.logPaxosStart(paxosID, nodeIDs, new StatePacket(new Ballot(0,0),0, initialState));
      }
      if(StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tBefore creating replica.");

      r = new PaxosReplica(paxosID, nodeID, nodeIDs);

//            paxosOccupancy.put(paxosID, 1);

      // paxosInstance object can be concurrently modified.
      paxosInstances.put(paxosID, r);
      if (StartNameServer.debugMode) GNS.getLogger().fine("Paxos instance created. Paxos ID = " + paxosID);
    }

    if(StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tStarting replica");
    if (r!= null) r.startReplica();

    if(StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tExit create Paxos");
    return true;
  }

  public static void deletePaxosInstance(String paxosID) {
    PaxosReplica replica = paxosInstances.remove(paxosID);
    if (replica != null)
      PaxosLogger2.logPaxosStop(paxosID);
  }

//	/**
//	 * Delete state corresponding to a Paxos instance.
//	 */
//	public static void deletePaxosInstance(String paxosID) {
//
//        PaxosReplica r = paxosInstances.remove(paxosID);
//
////        synchronized (paxosMessages) {
////            paxosMessages.remove(paxosID);
////            paxosOccupancy.remove(paxosID);
////
////        }
//		if (r != null) {
//			r.deleteState();
//			if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS INSTANCE DELETED. PaxosID = " + paxosID);
//		}
//		else {
//			if (StartNameServer.debugMode) GNS.getLogger().fine("PAXOS INSTANCE DOES NOT EXIST, DELETED ALREADY. PaxosID = " + paxosID);
//		}
//	}


//	/**
//	 * Check if the given proposed value is a stop command.
//	 * @param value command
//	 * @return true if command is a stop command, else false.
//	 */
//	static boolean isStopCommand(String value) {
//		// not a NO-OP
//		if (PaxosReplica.NO_OP.equals(value)) return false;
//		// is value == STOP, means stop command
//		if (PaxosReplica.STOP.equals(value)) return true;
//		// this code for l
//
//		if (!PaxosManager.debug) { // running as a part of GNS
//			try
//			{
//				JSONObject json = new JSONObject(value);
//				if (Packet.getPacketType(json).equals(Packet.PacketType.ACTIVE_PAXOS_STOP) ||
//						Packet.getPacketType(json).equals(Packet.PacketType.PRIMARY_PAXOS_STOP))
//					return true;
//			} catch (JSONException e)
//			{
//				if (StartNameServer.debugMode) GNS.getLogger().fine("ERROR: JSON Exception Here: " + e.getMessage());
//				e.printStackTrace();
//			}
//		}
//		return false;
//	}

  /**
   * Propose requestPacket in the paxos instance with paxosID.
   * ReqeustPacket.clientID is used to distinguish which method proposed this value.
   * @param paxosID
   * @param requestPacket
   */
  public static void propose(String paxosID, RequestPacket requestPacket) {

    try
    {
      JSONObject json = requestPacket.toJSONObject();
//			if (StartNameServer.debugMode) GNRS.getLogger().fine("PAXOS PROPOSE:" + json.toString());
      // put paxos ID for identification
      json.put(PAXOS_ID, paxosID);
//			if (StartNameServer.debugMode) GNRS.getLogger().fine("PAXOS PROPOSE:" + json.toString());
      handleIncomingPacket(json);

    } catch (JSONException e)
    {
      if (StartNameServer.debugMode) GNS.getLogger().fine(" JSON Exception" + e.getMessage());
      e.printStackTrace();
    }

  }

  /**
   * check if the failure detector has reported this node as up
   * @param nodeID
   */
  public static boolean isNodeUp(int nodeID) {
    return FailureDetection.isNodeUp(nodeID);
  }


//    static int decisionCount = 0;
//    static final  Object decisionLock = new ReentrantLock();
  /**
   *
   * @param paxosID
   * @param req
   */
  static void handleDecision(String paxosID, RequestPacket req)
  {
    clientRequestHandler.handlePaxosDecision(paxosID, req);
//    if (paxosInstances.containsKey(paxosID)) {
//
//    }
//    else {
//      if (StartNameServer.debugMode) GNS.getLogger().severe(nodeID + " Paxos ID not found: " + paxosID);
//    }

    if (req.isStopRequest()) {
      paxosInstances.remove(paxosID);
    }

  }

  /**
   * If a node fails, or comes up again, the respective Paxos instances are informed.
   * Some of them may elect a new co-ordinator.
   */
  static void informNodeStatus(FailureDetectionPacket fdPacket) {
    for (String x: paxosInstances.keySet()) {
      PaxosReplica r = paxosInstances.get(x);

      if (r.isNodeInPaxosInstance(fdPacket.responderNodeID)) {
        try
        {
          JSONObject json = fdPacket.toJSONObject();
          json.put(PAXOS_ID, x);
          processMessage(new HandlePaxosMessageTask(json,fdPacket.packetType));
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

  /**
   * Handle incoming message, incoming message could be of any Paxos instance.
   * @param json
   */
  public static void handleIncomingPacket(JSONObject json) {

    int incomingPacketType;
    try {

      if (StartNameServer.debugMode) GNS.getLogger().finer("recvd msg: " + json.toString());

      incomingPacketType = json.getInt(PaxosPacketType.ptype);
    } catch (JSONException e) {
      e.printStackTrace();
      return;
    }
    switch (incomingPacketType){
      case PaxosPacketType.DECISION:
//            case PaxosPacketType.ACCEPT:
//            case PaxosPacketType.PREPARE:
        try {
          PaxosLogger2.logMessage(new LoggingCommand(json.getString(PAXOS_ID),json, LoggingCommand.LOG_AND_EXECUTE));
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

//        if (json.has(PAXOS_ID)) {
////            if (StartNameServer.debugMode) GNS.getLogger().fine(paxosid + "\tPAXOS RECVD MSG: " + json);
//
//
//            if (incomingPacketType == PaxosPacketType.DECISION || incomingPacketType == PaxosPacketType.ACCEPT) {
//
//
//            } else {
//
//            }
//        }
//        else { // Failure detection packet
//
//        }

  }

  static void processMessage(Runnable runnable) {
    if (executorService!= null) executorService.submit(runnable);
  }

  static void sendMessage(int destID, JSONObject json, String paxosID) {
    try {
      json.put(PaxosManager.PAXOS_ID, paxosID);
      sendMessage(destID, json);
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

  }
  /**
   * all paxos instances use this method to exchange messages
   * @param destID
   * @param json
   */
  static void sendMessage(int destID, JSONObject json) {
    try
    {
      if (!debug) {
        Packet.putPacketType(json, Packet.PacketType.PAXOS_PACKET);

      }
      tcpTransport.sendToID(destID, json);
    } catch (IOException e)
    {
      if (StartNameServer.debugMode) GNS.getLogger().severe("Paxos: IO Exception in sending to ID. " + destID);
    } catch (JSONException e)
    {
      if (StartNameServer.debugMode) GNS.getLogger().severe("JSON Exception in sending to ID. " + destID);
    }

  }

  /**
   * main funtion to test the paxos manager code.
   * @param args
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
//            try {
      JSONObject json = (JSONObject)j;
      PaxosManager.handleIncomingPacket(json);
//                int incomingPacketType = json.getInt(PaxosPacketType.ptype);
//                if (incomingPacketType == PaxosPacketType.REMOVE) {
//                    String paxosID = json.getString(PaxosManager.PAXOS_ID);
//                    RequestPacket req = new RequestPacket(json);
//                    PaxosManager.clientRequestHandler.proposeRequestToPaxos(paxosID, req);
//                }
//                else {
//                    PaxosManager.handleIncomingPacket(json);
//                }
//            } catch (JSONException e) {
//                if (StartNameServer.debugMode) GNS.getLogger().fine("JSON Exception: PaxosPacketType not found");
//                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//                continue;
//            }

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
    try {
      String paxosID;
      try {
        paxosID = json.getString(PaxosManager.PAXOS_ID);
      } catch (JSONException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        return;
      }
      PaxosReplica replica = PaxosManager.paxosInstances.get(paxosID);
      if (replica != null) {
//                if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tPAXOS PROCESS START " + paxosID + "\t" +  json);
        replica.handleIncomingMessage(json,packetType);

//                if (StartNameServer.debugMode) GNS.getLogger().fine(paxosID + "\tPAXOS MSG DONE " + paxosID + "\t" +  json);
      }
      else {
        if (StartNameServer.debugMode) GNS.getLogger().fine("ERROR: Paxos Instances does not contain ID = " + paxosID);
      }
    } catch (Exception e) {
      if (StartNameServer.debugMode) GNS.getLogger().severe(" PAXOS EXCEPTION!!.");
      e.printStackTrace();
    }

  }
}

class ResendPendingMessagesTask extends TimerTask{

  @Override
  public  void run() {
    for (String x:PaxosManager.paxosInstances.keySet()) {
      PaxosReplica paxosReplica = PaxosManager.paxosInstances.get(x);
      if (paxosReplica !=null) {
        paxosReplica.resendPendingAccepts();
        paxosReplica.checkIfReplicasUptoDate();
      }
    }
  }
}


class CheckPaxosRecoveryCompleteTask extends TimerTask {

  @Override
  public void run() {
    PaxosManager.checkLogSynchronizationOver();
  }
}

/**
 * periodically logs state of all paxos instances
 */
class LogPaxosStateTask extends TimerTask {

  @Override
  public void run() {
    for (String x: PaxosManager.paxosInstances.keySet()) {
//      if (PaxosLogger2.resetStateChanged(x)) { //
      PaxosReplica r = PaxosManager.paxosInstances.get(x);
      if (x != null) {
        StatePacket packet = r.getState();
        PaxosLogger2.logPaxosState(x,packet);
      }
//      }
    }
  }
}
