package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.nio.InterfaceNodeConfig;
import edu.umass.cs.gns.nio.deprecated.ByteStreamToJSONObjects;
import edu.umass.cs.gns.nio.deprecated.NioServer;
import edu.umass.cs.gns.nsdesign.PacketTypeStamper;
import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.paxos.paxospacket.FailureDetectionPacket;
import edu.umass.cs.gns.paxos.paxospacket.PaxosPacket;
import edu.umass.cs.gns.paxos.paxospacket.PaxosPacketType;
import static edu.umass.cs.gns.paxos.paxospacket.PaxosPacketType.*;
import edu.umass.cs.gns.paxos.paxospacket.RequestPacket;
import edu.umass.cs.gns.paxos.paxospacket.StatePacket;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Manages a set of Paxos instances, each instance could be running 
 * on a different subset of nodes but including this node.
 * @author abhigyan
 *
 */
public class PaxosManager<NodeIDType> extends AbstractPaxosManager<NodeIDType> {

  static final String PAXOS_ID = "PXS";

  /**
   * nodeID of this node.
   */
  NodeIDType nodeID;
  
  InterfaceNodeConfig<NodeIDType> nodeConfig;

  /**
   * When paxos is run independently {@code nioServer} is used to send messages between paxos replicas and client.
   */
  private InterfaceJSONNIOTransport<NodeIDType> nioServer;

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
  final ConcurrentHashMap<String, PaxosReplicaInterface<NodeIDType>> paxosInstances;

  Replicable clientRequestHandler;

  ScheduledThreadPoolExecutor executorService;

  /**
   *
   */
  final TreeSet<ProposalStateAtCoordinator<NodeIDType>> proposalStates = new TreeSet<ProposalStateAtCoordinator<NodeIDType>>();

  /**
   * Paxos logs are garbage collected at this interval
   */
  static long PAXOS_LOG_STATE_INTERVAL_SEC = 100000;

  /**
   * Redundant paxos logs are checked and deleted at this interval.
   */
  static long PAXOS_LOG_DELETE_INTERVAL_SEC = 100000;

  /**
   * Minimum interval (in milliseconds) between two garbage state collections of replicas.
   */
  static int MEMORY_GARBAGE_COLLECTION_INTERVAL = 1000;

  /**
   * Paxos coordinator checks whether all replicas have received the latest messages decided.
   */
  static long RESEND_PENDING_MSG_INTERVAL_MILLIS = 2000;

  static int MAX_RESENDS = 5;


  /**
   * debug = true is used to debug the paxos module,  debug = false when complete GNRS system is running.
   */
  private  boolean test = false;

  PaxosLogger<NodeIDType> paxosLogger;

  FailureDetection<NodeIDType> failureDetection;

  private boolean initialized = false;

  public boolean isDebugMode() {
    return debugMode;
  }

  private boolean debugMode = true;

  private boolean consistentHashCoordinatorOrder = false;

  /********************BEGIN: public methods for paxos manager********************/

  public PaxosManager(NodeIDType nodeID, InterfaceNodeConfig<NodeIDType> nodeConfig, InterfaceJSONNIOTransport<NodeIDType> nioServer,
                              Replicable outputHandler, PaxosConfig paxosConfig) {

    this.executorService = new ScheduledThreadPoolExecutor(2);
    // set to false to cancel non-periodic delayed tasks upon shutdown
    this.executorService.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    this.nodeID = nodeID;
    this.nodeConfig = nodeConfig;
    assert nioServer instanceof PacketTypeStamper;
    this.nioServer = nioServer;
    this.clientRequestHandler = outputHandler;
    this.debugMode = paxosConfig.isDebugMode();
    this.consistentHashCoordinatorOrder = paxosConfig.isConsistentHashCoordinatorOrder();
    paxosLogger = new PaxosLogger<NodeIDType>(paxosConfig.getPaxosLogFolder(), nodeID, nodeConfig, this);
    long t0 = System.currentTimeMillis();
    ConcurrentHashMap<String, PaxosReplicaInterface<NodeIDType>> myPaxosInstances = paxosLogger.recoverPaxosLogs();

    long t1 = System.currentTimeMillis();
    GNS.getLogger().info("Time to recover paxos logs ... " + (t1 - t0)/1000 + " seconds");
    if (myPaxosInstances != null) paxosInstances = myPaxosInstances;
    else  paxosInstances = new ConcurrentHashMap<String, PaxosReplicaInterface<NodeIDType>>();

    paxosLogger.start();
    if (debugMode) GNS.getLogger().fine("Paxos instances: " + paxosInstances.size());

    GNS.getLogger().info("Paxos manager initialization complete");

    failureDetection = new FailureDetection<NodeIDType>(nodeConfig.getNodeIDs(), nodeID, executorService, this,
            paxosConfig.getFailureDetectionPingMillis(), paxosConfig.getFailureDetectionTimeoutMillis());
    startAllPaxosReplicas();
    startPaxosMaintenanceActions();
    initialized = true;
  }

  @Override
  public boolean createPaxosInstance(String paxosIDNoVersion, short version, Set<NodeIDType> nodeIDs, Replicable pi) {
    String initialState = pi.getState(paxosIDNoVersion);
    return createPaxosInstance(paxosIDNoVersion, version, nodeIDs, initialState);
  }


  @Override
  public Set<NodeIDType> getPaxosNodeIDs(String paxosIDNoVersion) {
    PaxosReplicaInterface<NodeIDType> replica = paxosInstances.get(paxosIDNoVersion);
    if (replica != null) {
      return replica.getNodeIDs();
    }
    return null;
  }


  /*** Adds a new Paxos instance to the set of actives. */
  public boolean createPaxosInstance(String paxosIDNoVersion, int version, Set<NodeIDType> nodeIDs, String initialState) {

    String paxosID = getPaxosIDWithVersionNumber(paxosIDNoVersion, version);

    if(debugMode) GNS.getLogger().info(paxosID + "\tEnter createPaxos");
    if (nodeIDs.size() < 3) {
      GNS.getLogger().severe(nodeID.toString() + " less than three replicas paxos instance cannot be created. SEVERE ERROR. " +
              "EXCEPTION Exception.");
      return false;
    }

    if (!nodeIDs.contains(nodeID)) {
      GNS.getLogger().severe(nodeID.toString() + " this node not a member of paxos instance. replica not created.");
      return false;
    }

    PaxosReplicaInterface<NodeIDType> r;

    PaxosReplicaInterface<NodeIDType> r1;
    synchronized (paxosInstances) { // paxosInstance object can be concurrently modified.
      r1 = paxosInstances.get(getPaxosKeyFromPaxosID(paxosID));

      if (r1 != null && r1.getPaxosID().equals(paxosID)) {
        GNS.getLogger().warning("Paxos replica already exists .. " + paxosID);
        return false;

      } else {

        assert initialState != null;
        r = createPaxosReplicaObject(paxosID, nodeID, nodeIDs);

        paxosLogger.logPaxosStart(paxosID, nodeIDs, new StatePacket(r.getAcceptorBallot(), 0, initialState));

        if(debugMode) GNS.getLogger().info(paxosID + "\tBefore creating replica.");
        paxosInstances.put(getPaxosKeyFromPaxosID(paxosID), r);
      }
    }

    if (r1 != null && !r1.getPaxosID().equals(paxosID)) {
      r1.removePendingProposals();
      GNS.getLogger().info("OldPaxos replica replaced .. so log a stop message .. " + r1.getPaxosID() + " new replica " + paxosID);
      paxosLogger.logPaxosStop(r1.getPaxosID());  // multiple stop msgs can toString logged because other replica might stop in meanwhile.
    }

    if (r!= null) {
      r.checkCoordinatorFailure(); // propose new ballot if default coordinator has failed
    }
    return true;
  }

  public  void resetAll() {
    // delete all paxos instances
    paxosInstances.clear();
    // clear paxos logs
    paxosLogger.clearLogs();
  }

  /**
   *
   */
  void startResendPendingMessages() {
    ResendPendingMessagesTask<NodeIDType> task = new ResendPendingMessagesTask<NodeIDType>(this);
    // single time execution
    executorService.scheduleAtFixedRate(task, RESEND_PENDING_MSG_INTERVAL_MILLIS,
            RESEND_PENDING_MSG_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
  }

  PaxosReplicaInterface<NodeIDType> createPaxosReplicaObject(String paxosID, NodeIDType nodeID, Set<NodeIDType> nodeIDs1) {
    return new PaxosReplica<NodeIDType>(paxosID, nodeID, nodeConfig, nodeIDs1, this);
  }

  @Override
  public String proposeStop(String paxosIDNoVersion, String value, short version) {
    PaxosReplicaInterface<NodeIDType> replica = paxosInstances.get(paxosIDNoVersion);
    String paxosIDWithVersion = getPaxosIDWithVersionNumber(paxosIDNoVersion, version);
    if (replica == null || !replica.getPaxosID().equals(paxosIDWithVersion)) {
      return null;
    }
    try {
      if (debugMode) GNS.getLogger().fine(" Proposing to  " + replica.getPaxosID());
      replica.handleIncomingMessage(new RequestPacket<NodeIDType>(null, value, 
              PaxosPacketType.REQUEST, true).toJSONObject(), PaxosPacketType.REQUEST.getInt());
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return replica.getPaxosID();
  }

  @Override
  public String propose(String paxosIDNoVersion, String value) {
    RequestPacket<NodeIDType> requestPacket = new RequestPacket<NodeIDType>(null, value, PaxosPacketType.REQUEST, false);
//    return propose(paxosIDNoVersion,new RequestPacket(0, value, PaxosPacketType.REQUEST, false));
    PaxosReplicaInterface replica = paxosInstances.get(paxosIDNoVersion);
    if (replica == null) {
      return null;
    }
    try {
      if (debugMode) GNS.getLogger().info(" Proposing to  " + replica.getPaxosID());
      replica.handleIncomingMessage(requestPacket.toJSONObject(), PaxosPacketType.REQUEST.getInt());
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return replica.getPaxosID();
  }

  /**
   * check if the failure detector has reported this node as up
   * @param nodeID  ID of node to be tested
   * @return <code>true</code> if failure detector tells node is up
   */
  boolean isNodeUp(NodeIDType nodeID) {
    return failureDetection == null || failureDetection.isNodeUp(nodeID);
  }


  /**
   * Handle incoming message, incoming message could be of any Paxos instance.
   * @param json json object received
   */
  @Override
  public  void handleIncomingPacket(JSONObject json) {

    int incomingPacketType;
    try {
      incomingPacketType = json.getInt(PaxosPacket.PACKET_TYPE_FIELD_NAME);
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem parsing JSON Object:" + json.toString());
      e.printStackTrace();
      return;
    }
    switch (PaxosPacketType.getPacketType(incomingPacketType)){

      case FAILURE_DETECT:
      case FAILURE_RESPONSE:
        processMessage(new HandleFailureDetectionPacketTask<NodeIDType>(json, failureDetection, nodeConfig));
        break;
      default:

        long t0 = System.currentTimeMillis();
        try {
          String paxosID;
          try {
            paxosID = json.getString(PaxosManager.PAXOS_ID);
          } catch (JSONException e) {
            e.printStackTrace();
            return;
          }

          PaxosReplicaInterface replica = paxosInstances.get(getPaxosKeyFromPaxosID(paxosID));
          if (replica != null && replica.getPaxosID().equals(paxosID)) {
            replica.handleIncomingMessage(json, incomingPacketType);
          }
          else {
            // this case can arise just before (after) a paxos instance is created (stopped).
            GNS.getLogger().warning("ERROR: Paxos Instances does not contain ID = " + paxosID + " Message: " + json);
          }
        } catch (Exception e) {
          GNS.getLogger().severe(" PAXOS Exception EXCEPTION!!. Msg = " + json);
          e.printStackTrace();
        }
        long t1 = System.currentTimeMillis();
        if (t1 - t0 > 100) {
          if (json.toString().length() < 1000) {
            GNS.getLogger().warning("Long delay " + (t1 - t0) + "ms. Packet: " + json);
          }
          else {
            GNS.getLogger().warning("Long delay " + (t1 - t0) + "ms.");
          }
        }
        break;
    }
  }

  /**
   *
   * @param paxosID paxosID of the paxos group
   * @param req request that is to be executed
   */
  void handleDecision(String paxosID, RequestPacket req, boolean recovery) {
    clientRequestHandler.handleDecision(getPaxosIDNoVersion(paxosID), req.value, recovery);
  }

  String getPaxosKeyFromPaxosID(String paxosID) {
    return getPaxosIDNoVersion(paxosID);
  }

  private String getPaxosIDWithVersionNumber(String paxosIDNoVersion, int version) {
    return paxosIDNoVersion + ":" + version;
  }

  private String getPaxosIDNoVersion(String paxosID) {
    int index = paxosID.lastIndexOf(":");
    if (index == -1) return paxosID;
    return paxosID.substring(0, index);
  }

  /**
   * If a node fails, or comes up again, the respective Paxos instances are informed.
   * Some of them may elect a new coordinator.
   */
  void informNodeStatus(FailureDetectionPacket<NodeIDType> fdPacket) {
    GNS.getLogger().severe("Handling node failure = " + fdPacket.responderNodeID);
    for (String x: paxosInstances.keySet()) {
      PaxosReplicaInterface<NodeIDType> r = paxosInstances.get(x);

      if (r.isNodeInPaxosInstance(fdPacket.responderNodeID)) {
        try
        {
          r.handleIncomingMessage(fdPacket.toJSONObject(), fdPacket.packetType);
        } catch (JSONException e)
        {
          if (debugMode) GNS.getLogger().fine(" JSON Exception");
          e.printStackTrace();
        }
      }
    }
  }

  void sendMessage(NodeIDType destID, JSONObject json, String paxosID) {
    try {
      json.put(PaxosManager.PAXOS_ID, paxosID);
      sendMessage(destID, json);
    } catch (JSONException e) {
      e.printStackTrace();  
    }
  }

  void sendMessage(Set<NodeIDType> destIDs, JSONObject json, String paxosID) {
    try {
      json.put(PaxosManager.PAXOS_ID, paxosID);
      sendMessage(destIDs, json);
    } catch (JSONException e) {
      e.printStackTrace();  
    }
  }

  void addToActiveProposals(ProposalStateAtCoordinator<NodeIDType> propState) {
    synchronized (proposalStates) {
      proposalStates.add(propState);
    }
  }

  void removeFromActiveProposals(ProposalStateAtCoordinator<NodeIDType> propState) {
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
      for (String x: paxosInstances.keySet()) {
        if (debugMode) GNS.getLogger().fine("Paxos Recovery: starting paxos replica .. " + x);
        paxosInstances.get(x).checkInitScout();
      }
  }


  /**
   * Create a paxos instance for testing/debugging.
   */
  @SuppressWarnings("unchecked")
  private  void createTestPaxosInstance(String testPaxosID, int N) {
    if (paxosInstances.containsKey(getPaxosKeyFromPaxosID(testPaxosID))) {
      if (debugMode) GNS.getLogger().fine("Paxos instance " + testPaxosID + " already exists.");
      return;
    }
    // create a default paxos instance for testing.
    Set<NodeIDType> x = new HashSet<NodeIDType>();
    for (int i = 0;  i < N; i++)
      x.add((NodeIDType)Integer.toString(i));
    createPaxosInstance(testPaxosID, 0, x, clientRequestHandler.getState(testPaxosID));
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
      GNS.getLogger().fine(" Node ID is " + nodeID.toString());

      ByteStreamToJSONObjects jsonMessageWorker = new ByteStreamToJSONObjects(paxosDemux);
      tcpTransportLocal = new NioServer<NodeIDType>(nodeID, jsonMessageWorker, config);
//      JSONMessageWorker jsonMessageWorker = new JSONMessageWorker(paxosDemux);
//      tcpTransportLocal = new GNSNIOTransport(nodeID, config, jsonMessageWorker);


      if (debugMode) GNS.getLogger().fine(" TRANSPORT OBJECT CREATED for node  " + nodeID.toString());

      // start TCP transport thread
      new Thread(tcpTransportLocal).start();
    } catch (IOException e) {
      GNS.getLogger().severe(" Could not initialize TCP socket at client");
      e.printStackTrace();  
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
  private void sendMessage(NodeIDType destID, JSONObject json) {
    try
    {
      if (!test) {
        Packet.putPacketType(json, Packet.PacketType.PAXOS_PACKET);
      }
//      GNS.getLogger().fine("Sending message to " + destID + "\t" + json);
      if (initialized) nioServer.sendToID(destID, json);
    } catch (IOException e)
    {
      GNS.getLogger().severe("Paxos: IO Exception in sending to ID. " + destID);
    } catch (JSONException e)
    {
      GNS.getLogger().severe("JSON Exception in sending to ID. " + destID);
    }
  }

  private  void sendMessage(Set<NodeIDType> destIDs, JSONObject json) {
    try {
      if (!test) {
        Packet.putPacketType(json, Packet.PacketType.PAXOS_PACKET);
      }
      if (initialized) {
        for (NodeIDType x: destIDs)
          nioServer.sendToID(x, json);
      }
    } catch (IOException e) {
      GNS.getLogger().severe("Paxos: IO Exception in sending to IDs. " + destIDs);
    } catch (JSONException e) {
      GNS.getLogger().severe("JSON Exception in sending to IDs. " + destIDs);
    }
  }

  public boolean isConsistentHashCoordinatorOrder() {
    return consistentHashCoordinatorOrder;
  }

  public void shutdown() {
    this.executorService.shutdownNow();
    try {
      executorService.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assert executorService.isTerminated();
    GNS.getLogger().warning("Paxos internal executor thread shutting down.");
    this.paxosLogger.shutdown();
    GNS.getLogger().warning("Paxos logger shut down");
  }
}

/**
 * PaxosPacket demultiplexer object
 */
class PaxosPacketDemultiplexer extends AbstractPacketDemultiplexer {

  PaxosManager paxosManager;

  public PaxosPacketDemultiplexer(PaxosManager paxosManager) {
    this.paxosManager = paxosManager;
  }

  @Override
  public boolean handleJSONObject(JSONObject jsonObject) {
    paxosManager.handleIncomingPacket(jsonObject);
    return true;
  }
}

/**
 * Resend proposals (for all paxos instances) that have not yet been accepted by majority.
 */
class ResendPendingMessagesTask<NodeIDType> extends TimerTask{

  PaxosManager<NodeIDType> paxosManager;

  public ResendPendingMessagesTask(PaxosManager<NodeIDType> paxosManager) {
    this.paxosManager = paxosManager;
  }

  @Override
  public void run() {
    // requests that will be reattempted
    ArrayList<ProposalStateAtCoordinator<NodeIDType>> reattempts = new ArrayList<ProposalStateAtCoordinator<NodeIDType>>();
    // requests that will be removed because they have already been tried  'MAX_RESENDS' times
    ArrayList<ProposalStateAtCoordinator<NodeIDType>> removeList = new ArrayList<ProposalStateAtCoordinator<NodeIDType>>();

    try{
      //
      synchronized (paxosManager.proposalStates){
        for (ProposalStateAtCoordinator<NodeIDType> propState: paxosManager.proposalStates) {
          if (propState.getResendCount() >= PaxosManager.MAX_RESENDS) {
            removeList.add(propState);
          }
          else if (propState.getTimeSinceAccept() > PaxosManager.RESEND_PENDING_MSG_INTERVAL_MILLIS) {
            reattempts.add(propState);
          }
          else break;
        }
      }

      for (ProposalStateAtCoordinator<NodeIDType> propState: removeList) {
        synchronized (paxosManager.proposalStates){
          paxosManager.proposalStates.remove(propState);
        }
        GNS.getLogger().severe("Stopping to resend proposal!! Pray that it gets accepted. "
                + " PaxosID " + propState.paxosReplica.getPaxosID()
                + " pValuePacket " + propState.pValuePacket);
      }

      for (ProposalStateAtCoordinator<NodeIDType> propState: reattempts) {
        boolean result = propState.paxosReplica.resendPendingProposal(propState);
        if (!result) {
          synchronized (paxosManager.proposalStates){
            paxosManager.proposalStates.remove(propState);
          }
        } else {
          propState.increaseResendCount();
          GNS.getLogger().warning("\tResendingMessage\t" + propState.paxosReplica.getPaxosID() + "\t" +
                  propState.pValuePacket.proposal.slot + "\t");
        }
      }
    }catch (Exception e) {
      GNS.getLogger().severe("Exception in sending pending messages." + e.getMessage());
      e.printStackTrace();
    }

  }
}
